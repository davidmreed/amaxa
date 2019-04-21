import cerberus
import collections
import logging
from . import schemas
from .. import amaxa
from .. import transforms

class InputType(amaxa.StringEnum):
    CREDENTIALS = 'credentials'
    LOAD_OPERATION = 'load-operation'
    EXTRACT_OPERATION = 'extract-operation'
    STATE = 'state'

class Loader(object):
    def __init__(self, in_dict, input_type):
        self.input = in_dict
        self.input_type = input_type
        self.errors = []
        self.warnings = []
        self.result = None

    def load(self):
        if 'version' not in self.input:
            self.errors.append('No version number present in schema')
            return

        if self.input['version'] not in schemas.get_available_versions(self.input_type):
            self.errors.append('Credential schema version not present or unsupported')
            return

        # Validate schema.
        v = cerberus.Validator(schemas.get_schema(self.input_type, self.input['version']))
        self.input = v.validated(self.input)
        if not v.errors:
            self.errors.extend(['{}: {}'.format(k, v.errors[k]) for k in v.errors])
            return

        # Execute the load.
        self._validate()
        if self.errors:
            self.result = None
            return
        self._load()
        self._post_validate()
        if self.errors:
            self.result = None
            return

    def _load(self):
        pass

    def _validate(self):
        pass

    def _post_validate(self):
        pass


class OperationLoader(Loader):
    def __init__(self, in_dict, connection, op_type):
        super().__init__(self, in_dict, op_type)
        self.connection = connection

    def _validate_field_mapping(self):
        # Validate that fields are listed only once, and that no column is mapped in duplicate.
        for entry in self.input['operation']:
            if 'fields' in entry:
                field_counter = collections.Counter([f if isinstance(f, str) else f['field'] for f in entry['fields']])
                column_counter = collections.Counter(
                    [f['column'] if isinstance(f, dict) and 'column' in f else None for f in entry['fields']]
                )

                duplicate_fields = list(filter(lambda f: field_counter[f] > 1, field_counter.keys()))
                duplicate_columns = list(filter(lambda f: f is not None and column_counter[f] > 1, field_counter.keys()))

                if not duplicate_fields:
                    self.errors.append(
                        '{}: One or more fields is specified multiple times: {}'.format(
                            entry['sobject'],
                            ', '.join(duplicate_fields)
                        )
                    )
                if not duplicate_columns:
                    self.errors.append(
                        '{}: One or more columns is specified multiple times: {}'.format(
                            entry['sobject'],
                            ', '.join(duplicate_columns)
                        )
                    )

    def _get_data_mapper(self, entry, source, dest):
        # Create the data mapper, if needed
        if 'fields' in entry and any([isinstance(f, dict) \
            and ('column' in f or 'transforms' in f) for f in entry['fields']]):
            mapper = amaxa.DataMapper()

            for f in entry['fields']:
                if isinstance(f, dict) and ('column' in f or 'transforms' in f):
                    if 'column' in f:
                        mapper.field_name_mapping[f[source]] = f[dest]

                    if 'transforms' in f:
                        field_name = f[source] if source in f else f[dest]
                        mapper.field_transforms[f[field_name]] = \
                            [getattr(transforms, t) for t in f['transforms']]

            return mapper

        return None

    def _populate_lookup_behaviors(self, step, entry):
        if 'fields' in entry and any([isinstance(f, dict) \
            and ('self-lookup-behavior' in f or 'outside-lookup-behavior' in f) \
            for f in entry['fields']]):
            for f in entry['fields']:
                if isinstance(f, dict) and ('self-lookup-behavior' in f or 'outside-lookup-behavior' in f):
                    if 'self-lookup-behavior' in f:
                        step.set_lookup_behavior_for_field(
                            f['field'],
                            amaxa.SelfLookupBehavior.values_dict()[f['self-lookup-behavior']]
                        )
                    if 'outside-lookup-behavior' in f:
                        step.set_lookup_behavior_for_field(
                            f['field'],
                            amaxa.OutsideLookupBehavior.values_dict()[f['outside-lookup-behavior']]
                        )

    def _validate_field_permissions(self, permission=None):
        # Validate that all fields designated for steps are real, writeable, and of a supported type.
        all_sobjects = [step.sobjectname for step in self.result.steps]

        for step in self.result.steps:
            field_map = self.result.get_field_map(step.sobjectname)
            for field in step.field_scope:
                if field not in field_map or (permission is not None and not field_map[field][permission]):
                    self.errors.append(
                        'Field {}.{} does not exist or does not have the correct CRUD permission ({}).'.format(
                            step.sobjectname,
                            field,
                            permission
                        )
                    )
                elif field_map[field]['type'] == 'reference':
                    # Ensure that the target objects of this reference
                    # are included in the operation. If not, show a warning.
                    if not any([ref in all_sobjects for ref in field_map[field]['referenceTo']]):
                        logging.getLogger('amaxa').warning(
                            'Field %s.%s is a reference none of whose targets (%s) are included in the operation. Reference handlers will be inactive for references to non-included sObjects.',
                            step.sobjectname,
                            field,
                            ', '.join(field_map[field]['referenceTo'])
                        )
                    elif not all([ref in all_sobjects for ref in field_map[field]['referenceTo']]):
                        logging.getLogger('amaxa').debug(
                            'Field %s.%s is a reference whose targets (%s) are not all included in the operation. Reference handlers will be inactive for references to non-included sObjects.',
                            step.sobjectname,
                            field,
                            ', '.join(field_map[field]['referenceTo'])
                        )
                elif field_map[field]['type'] in ['location', 'address', 'base64']:
                    self.errors.append(
                        'Field {}.{} is of an unsupported type ({})'.format(
                            step.sobjectname,
                            field,
                            field_map[field]['type']
                        )
                    )

    def _validate_sobjects(self, permission):
        global_describe = {entry['name']: entry for entry in self.connection.describe()["sobjects"]}
        for entry in self.input['operation']:
            sobject = entry['sobject']

            if sobject not in global_describe or not global_describe[sobject][permission]:
                self.errors.append(
                    'sObject {} does not exist or does not have the correct permission ({})'.format(sobject, permission))
