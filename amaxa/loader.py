import csv
import simple_salesforce
import cerberus
import logging
from . import amaxa
from . import constants
from . import transforms
from . import jwt_auth

def load_credentials(incoming, load):
    (credentials, errors) = validate_credential_schema(incoming)
    if credentials is None:
        return (None, errors)

    connection = None
    credentials = credentials['credentials']

    # Determine what type of credentials we have
    if 'username' in credentials and 'password' in credentials:
        # User + password, optional Security Token
        connection = simple_salesforce.Salesforce(
            username = credentials['username'],
            password = credentials['password'],
            security_token = credentials.get('security-token', ''),
            organizationId = credentials.get('organization-id', ''),
            sandbox = credentials.get('sandbox', False)
        )

        logging.getLogger('amaxa').debug('Authenticating to Salesforce with user name and password')
    elif 'username' in credentials and 'consumer-key' and 'jwt-key' in credentials:
        # JWT authentication with key provided inline.
        try:
            connection = jwt_auth.jwt_login(
                credentials['consumer-key'],
                credentials['username'],
                credentials['jwt-key'],
                credentials.get('sandbox', False)
            )
            logging.getLogger('amaxa').debug('Authenticating to Salesforce with inline JWT key')
        except simple_salesforce.exceptions.SalesforceAuthenticationFailed as e:
            return (None, ['Failed to authenticate with JWT: {}'.format(e.message)])
    elif 'username' in credentials and 'jwt-file' in credentials:
        # JWT authentication with external keyfile.
        try:
            with open(credentials['jwt-file'], 'r') as jwt_file:
                connection = jwt_auth.jwt_login(
                    credentials['consumer-key'],
                    credentials['username'],
                    jwt_file.read(),
                    credentials.get('sandbox', False)
                )
            logging.getLogger('amaxa').debug('Authenticating to Salesforce with external JWT key')
        except simple_salesforce.exceptions.SalesforceAuthenticationFailed as e:
            return (None, ['Failed to authenticate with JWT: {}'.format(e.message)])
    elif 'access-token' in credentials and 'instance-url' in credentials:
        connection = simple_salesforce.Salesforce(instance_url=credentials['instance-url'], 
                                                  session_id=credentials['access-token'])
        logging.getLogger('amaxa').debug('Authenticating to Salesforce with access token')
    else:
        return (None, ['A set of valid credentials was not provided.'])
    
    if not load:
        context = amaxa.ExtractOperation(connection)
    else:
        context = amaxa.LoadOperation(connection)
    
    return (context, [])

def load_load_operation(incoming, context, resume = False):
    # Inbound is raw, deserialized structures from JSON or YAML input files.
    # First, validate them against our schema and normalize them.

    (incoming, errors) = validate_load_schema(incoming)
    if incoming is None:
        return (None, errors)
    
    try:
        global_describe = { entry['name']: entry for entry in context.connection.describe()["sobjects"] }
    except Exception as e:
        errors.append('Unable to authenticate to Salesforce: {}'.format(e))
        return (None, errors)

    errors = []

    all_sobjects = [entry['sobject'] for entry in incoming['operation']]

    for entry in incoming['operation']:
        sobject = entry['sobject']

        if sobject not in global_describe or not global_describe[sobject]['createable']:
            errors.append('sObject {} does not exist, is not visible, or is not createable.'.format(sobject))
            continue

        # Determine the field scope
        lookup_behaviors = {}
        if 'field-group' in entry:
            # Validation clamps acceptable values to 'writeable' or 'smart' by this point.
            # Don't include field types we don't process - geolocations, addresses, and base64 fields.
            # Geolocations and addresses are omitted automatically (they aren't writeable)
            lam = lambda f: f['createable'] and f['type'] != 'base64'

            field_set = set(context.get_filtered_field_map(sobject, lam).keys())
        else:
            fields = entry.get('fields')
            mapped_columns = set()

            # Determine whether we are doing any mapping
            if any([isinstance(f, dict) for f in fields]):
                mapper = amaxa.DataMapper()
                field_set = set()
                for f in fields:
                    if isinstance(f, str):
                        if f in field_set:
                            errors.append('Field {}.{} is present more than once in the specification.'.format(sobject, f))
                        if f in mapped_columns:
                            errors.append('Column {} is mapped to field {}.{}, but this column is already mapped.'.format(f, sobject, f))

                        field_set.add(f)
                        mapped_columns.add(f)
                    else:
                        if f['field'] in field_set:
                            errors.append('Field {}.{} is present more than once in the specification.'.format(sobject, f['field']))

                        field_set.add(f['field'])

                        if 'column' in f:
                            if f['column'] in mapped_columns:
                                errors.append('Column {} is mapped to field {}.{}, but this column is already mapped.'.format(f['column'], sobject, f['field']))

                            # Note we reverse the mapper's dict for loads
                            mapper.field_name_mapping[f['column']] = f['field']
                            mapped_columns.add(f['column'])
                        if 'transforms' in f:
                            mapper.field_transforms[f['column']] = [getattr(transforms,t) for t in f['transforms']]
                        if 'self-lookup-behavior' in f:
                            lookup_behaviors[f['field']] = amaxa.SelfLookupBehavior.values_dict()[f['self-lookup-behavior']]
                        if 'outside-lookup-behavior' in f:
                            lookup_behaviors[f['field']] = amaxa.OutsideLookupBehavior.values_dict()[f['outside-lookup-behavior']]
                
                context.mappers[sobject] = mapper
            else:
                field_set = set(fields)

        # Validate that all fields are real and writeable by this user.
        field_map = context.get_field_map(sobject)
        for f in field_set:
            if f not in field_map or not field_map[f]['createable']:
                errors.append('Field {}.{} does not exist, is not writeable, or is not visible.'.format(sobject, f))
            elif field_map[f]['type'] == 'reference':
                # Ensure that the target objects of this reference
                # are included in the extraction. If not, show a warning.
                if not any([ref in all_sobjects for ref in field_map[f]['referenceTo']]):
                    logging.getLogger('amaxa').warning(
                        'Field %s.%s is a reference none of whose targets (%s) are included in the load. Reference handlers will be inactive for references to non-included sObjects.',
                        sobject,
                        f,
                        ', '.join(field_map[f]['referenceTo'])
                    )
                elif not all([ref in all_sobjects for ref in field_map[f]['referenceTo']]):
                    logging.getLogger('amaxa').debug(
                        'Field %s.%s is a reference whose targets (%s) are not all included in the load. Reference handlers will be inactive for references to non-included sObjects.',
                        sobject,
                        f,
                        ', '.join(field_map[f]['referenceTo'])
                    )
            elif field_map[f]['type'] == 'base64':
                # Compound fields aren't writeable, so will be handled by the branch above.
                errors.append('Field {}.{} is a base64 field, which is not supported.'.format(sobject, f))

        # If we've located any errors, continue to validate the rest of the extraction,
        # but don't actually create any steps or files.
        if len(errors) > 0:
            continue

        step = amaxa.LoadStep(
            sobject, 
            field_set, 
            amaxa.OutsideLookupBehavior.values_dict()[entry['outside-lookup-behavior']]
        )

        # Populate expected lookup behaviors
        for l in lookup_behaviors:
            step.set_lookup_behavior_for_field(l, lookup_behaviors[l])
        
        context.add_step(step)

    context.initialize()

    validate_dependent_field_permissions(context, errors)
    validate_lookup_behaviors(context.steps, errors)

    if len(errors) > 0:
        return (None, errors)
    
    # Open all of the input and output files
    # Create DictReaders and populate them in the context
    for (s, e) in zip(context.steps, incoming['operation']):
        try:
            fh = open(e['file'], 'r')
            input_file = csv.DictReader(fh)
            context.file_store.set_file(s.sobjectname, amaxa.FileType.INPUT, fh)
            context.file_store.set_csv(s.sobjectname, amaxa.FileType.INPUT, input_file)
        except Exception as exp:
            errors.append('Unable to open file {} for reading ({}).'.format(e['file'], exp))

        try:
            f = open(e['result-file'], 'w' if not resume else 'a')
            output = csv.DictWriter(
                f, 
                fieldnames=[constants.ORIGINAL_ID, constants.NEW_ID, constants.ERROR]
            )
            if not resume:
                output.writeheader()
            context.file_store.set_file(s.sobjectname, amaxa.FileType.RESULT, f)
            context.file_store.set_csv(s.sobjectname, amaxa.FileType.RESULT, output)
        except Exception as exp:
            errors.append('Unable to open file {} for writing ({})'.format(e['result-file'], exp))

    if len(errors) > 0:
        return (None, errors)

    # Validate the column sets in the input files.
    # For each file, if validation is active, check as follows.
    # For field group steps, validate that each column in the input file
    # is mapped to a field within the field group, but allow "missing" columns.
    # For explicit field list steps, require that the mapped column set and field scope be 1:1
    for (s, e) in zip(context.steps, incoming['operation']):
        if e['input-validation'] == 'none':
            continue

        input_file = context.file_store.get_csv(s.sobjectname, amaxa.FileType.INPUT)
        file_field_set = set(input_file.fieldnames)
        if 'Id' in file_field_set:
            file_field_set.remove('Id')

        # If we have transforms in place, transform all the column names into field names.
        if s.sobjectname in context.mappers:
            file_field_set = { context.mappers[s.sobjectname].transform_key(f) for f in file_field_set }

        if 'field-group' in e and e['input-validation'] == 'default':
            # Field group validation: file can omit columns but can't have extra
            # For the 'smart' field group, we permit any readable (but not writeable) fields
            # to be in the file, since the file was likely pulled with 'smart'=='readable'
            if e['field-group'] == 'smart':
                comparand = set(
                    context.get_filtered_field_map(
                        s.sobjectname,
                        lambda f: f['type'] not in ['location', 'address', 'base64']
                    ).keys()
                )
            else:
                comparand = s.field_scope

            if not comparand.issuperset(file_field_set):
                errors.append(
                    'Input file for sObject {} contains excess columns over field group \'{}\': {}'.format(
                        s.sobjectname,
                        e['field-group'],
                        ', '.join(sorted(file_field_set.difference(comparand)))
                    )
                )
        else:
            # Field scope validation, or strict-mode group validation.
            # File columns must match field scope precisely.
            if s.field_scope != file_field_set:
                errors.append(
                    'Input file for sObject {} does not match specified field scope.\nScope: {}\nFile Columns: {}\n'.format(
                        s.sobjectname,
                        ', '.join(sorted(s.field_scope)),
                        ', '.join(sorted(file_field_set))
                    )
                )

    if len(errors) > 0:
        return (None, errors)

    return (context, [])

def load_extraction_operation(incoming, context):
    # Inbound is raw, deserialized structures from JSON or YAML input files.
    # First, validate them against our schema and normalize them.

    (incoming, errors) = validate_extraction_schema(incoming)
    if incoming is None:
        return (None, errors)
    
    try:
        global_describe = { entry['name']: entry for entry in context.connection.describe()["sobjects"] }
    except Exception as e:
        errors.append('Unable to authenticate to Salesforce: {}'.format(e))
        return (None, errors)

    errors = []

    all_sobjects = [entry['sobject'] for entry in incoming['operation']]

    for entry in incoming['operation']:
        sobject = entry['sobject']

        if sobject not in global_describe or not global_describe[sobject]['retrieveable'] or not global_describe[sobject]['queryable']:
            errors.append('sObject {} does not exist or is not visible.'.format(sobject))
            continue

        # Determine the type of extraction
        query = None
        to_extract = entry.get('extract')

        if 'ids' in to_extract:
            # Register the required IDs in the context
            try:
                for id in to_extract.get('ids'):
                    context.add_dependency(sobject, amaxa.SalesforceId(id))
            except ValueError:
                errors.append('One or more invalid Id values provided for sObject {}'.format(sobject))
            
            scope = amaxa.ExtractionScope.SELECTED_RECORDS
        elif 'query' in to_extract:
            query = to_extract['query']
            scope = amaxa.ExtractionScope.QUERY
        elif 'all' in to_extract:
            scope = amaxa.ExtractionScope.ALL_RECORDS
        else:
            scope = amaxa.ExtractionScope.DESCENDENTS
        
        # Determine the field scope
        lookup_behaviors = {}
        if 'field-group' in entry:
            # If we're using a field group, filter by FLS and remove field types we don't handle.
            if entry['field-group'] in ['readable', 'smart']:
                lam = lambda f: f['type'] not in ['location', 'address', 'base64']
            else:
                lam = lambda f: f['createable'] and f['type'] not in ['location', 'address', 'base64']

            field_set = set(context.get_filtered_field_map(sobject, lam).keys())
        else:
            fields = entry.get('fields')
            mapped_columns = set()

            # Determine whether we are doing any mapping
            if any([isinstance(f, dict) for f in fields]):
                mapper = amaxa.DataMapper()
                field_set = set()
                for f in fields:
                    if isinstance(f, str):
                        if f in field_set:
                            errors.append('Field {}.{} is present more than once in the specification.'.format(sobject, f))

                        field_set.add(f)
                        if f not in mapped_columns:
                            mapped_columns.add(f)
                        else:
                            errors.append('Field {}.{} is mapped to column {}, but this column is already mapped.'.format(sobject, f, f))
                    else:
                        if f['field'] in field_set:
                            errors.append('Field {}.{} is present more than once in the specification.'.format(sobject, f['field']))

                        field_set.add(f['field'])
                        if 'column' in f:
                            mapper.field_name_mapping[f['field']] = f['column']
                            if f['column'] not in mapped_columns:
                                mapped_columns.add(f['column'])
                            else:
                                errors.append('Field {}.{} is mapped to column {}, but this column is already mapped.'.format(sobject, f['field'], f['column']))
                        if 'transforms' in f:
                            mapper.field_transforms[f['field']] = [getattr(transforms,t) for t in f['transforms']]
                        if 'self-lookup-behavior' in f:
                            lookup_behaviors[f['field']] = amaxa.SelfLookupBehavior.values_dict()[f['self-lookup-behavior']]
                        if 'outside-lookup-behavior' in f:
                            lookup_behaviors[f['field']] = amaxa.OutsideLookupBehavior.values_dict()[f['outside-lookup-behavior']]
                
                context.mappers[sobject] = mapper
            else:
                field_set = set(fields)

        field_set.add('Id')

        # Validate that all fields are real and readable by this user.
        field_map = context.get_field_map(sobject)
        for f in field_set:
            if f not in field_map:
                errors.append('Field {}.{} does not exist or is not visible.'.format(sobject, f))
            elif field_map[f]['type'] == 'reference':
                # Ensure that the target objects of this reference
                # are included in the extraction. If not, show a warning.
                if not any([ref in all_sobjects for ref in field_map[f]['referenceTo']]):
                    logging.getLogger('amaxa').warning(
                        'Field %s.%s is a reference none of whose targets (%s) are included in the extraction. Reference handlers will be inactive for references to non-included sObjects.',
                        sobject,
                        f,
                        ', '.join(field_map[f]['referenceTo'])
                    )
                elif not all([ref in all_sobjects for ref in field_map[f]['referenceTo']]):
                    logging.getLogger('amaxa').debug(
                        'Field %s.%s is a reference whose targets (%s) are not all included in the extraction. Reference handlers will be inactive for references to non-included sObjects.',
                        sobject,
                        f,
                        ', '.join(field_map[f]['referenceTo'])
                    )
            elif field_map[f]['type'] in ['location', 'address', 'base64']:
                errors.append('Field {}.{} is a {} field, which is not supported.'.format(sobject, f, field_map[f]['type']))

        # If we've located any errors, continue to validate the rest of the extraction,
        # but don't actually create any steps or files.
        if len(errors) > 0:
            continue

        step = amaxa.ExtractionStep(
            sobject, 
            scope, 
            field_set, 
            query,
            amaxa.SelfLookupBehavior.values_dict()[entry['self-lookup-behavior']],
            amaxa.OutsideLookupBehavior.values_dict()[entry['outside-lookup-behavior']]
        )

        # Populate expected lookup behaviors
        for l in lookup_behaviors:
            step.set_lookup_behavior_for_field(l, lookup_behaviors[l])
        
        context.add_step(step)

    for step in context.steps:
        step.initialize()
    validate_lookup_behaviors(context.steps, errors)

    if len(errors) > 0:
        return (None, errors)
    
    # Open all of the output files
    # Create DictWriters and populate them in the context
    for (s, e) in zip(context.steps, incoming['operation']):
        try:
            f = open(e['file'], 'w')
            fieldnames = s.field_scope if s.sobjectname not in context.mappers else [context.mappers[s.sobjectname].transform_key(k) for k in s.field_scope]
            output = csv.DictWriter(
                f,
                fieldnames = sorted(fieldnames, key=lambda x: x if x != 'Id' else ' Id'),
                extrasaction='ignore'
            )
            output.writeheader()
            context.file_store.set_file(s.sobjectname, amaxa.FileType.OUTPUT, f)
            context.file_store.set_csv(s.sobjectname, amaxa.FileType.OUTPUT, output)
        except Exception as exp:
            return (None, ['Unable to open file {} for writing ({}).'.format(e['file'], exp)])

    return (context, [])

def validate_dependent_field_permissions(context, errors):
    for step in context.steps:
        field_map = context.get_field_map(step.sobjectname)
        for f in step.dependent_lookups | step.self_lookups:
            if not field_map[f]['updateable']:
                errors.append('Field {}.{} is a dependent lookup, but is not updateable.'.format(step.sobjectname, f))


def validate_lookup_behaviors(steps, errors):
    # Scan fields for each step (populate the various lookup collections)
    # so we can validate the lookup behaviors.
    for step in steps:
        for f in step.lookup_behaviors:
            if (f in step.dependent_lookups and step.lookup_behaviors[f] not in amaxa.OutsideLookupBehavior) \
                or (f in step.self_lookups and step.lookup_behaviors[f] not in amaxa.SelfLookupBehavior):
                errors.append('Lookup behavior \'{}\' specified for field {}.{} is not valid for this lookup type.'.format(
                    step.lookup_behaviors[f].value,
                    step.sobjectname,
                    f
                ))

def validate_extraction_schema(input):
    v = cerberus.Validator(get_operation_schema(True))
    return (
        v.validated(input),
        ['{}: {}'.format(k, v.errors[k]) for k in v.errors]
    )

def validate_load_schema(input):
    v = cerberus.Validator(get_operation_schema(False))
    return (
        v.validated(input),
        ['{}: {}'.format(k, v.errors[k]) for k in v.errors]
    )

def validate_credential_schema(input):
    v = cerberus.Validator(credential_schema)
    return (
        v.validated(input),
        ['{}: {}'.format(k, v.errors[k]) for k in v.errors]
    )

credential_schema = {
    'version': {
        'type': 'integer',
        'required': True,
        'allowed': [1]
    },
    'credentials': {
        'type': 'dict',
        'required': True,
        'schema': {
            'username': {
                'type': 'string',
                'excludes': ['access-token', 'instance-url']
            },
            'sandbox': {
                'type': 'boolean',
                'default': False
            },
            'access-token': {
                'dependencies': ['instance-url'],
                'type': 'string',
                'excludes': ['username', 'password', 'security-token', 'jwt-key', 'jwt-file', 'consumer-key']
            },
            'password': {
                'dependencies': ['username'],
                'type': 'string',
                'excludes': ['access-token', 'instance-url', 'jwt-key', 'jwt-file', 'consumer-key']
            },
            'security-token': {
                'dependencies': ['username', 'password'],
                'type': 'string',
                'excludes': ['access-token', 'instance-url', 'jwt-key', 'jwt-file', 'consumer-key']
            },
            'organization-id': {
                'dependencies': ['username', 'password'],
                'type': 'string',
                'excludes': ['access-token', 'instance-url', 'jwt-key', 'jwt-file', 'consumer-key']
            },
            'instance-url': {
                'dependencies': ['access-token'],
                'type': 'string',
                'excludes': ['username', 'password', 'security-token', 'jwt-key', 'jwt-file', 'consumer-key']
            },
            'jwt-key': {
                'dependencies': ['consumer-key', 'username'],
                'type': 'string',
                'excludes': ['password', 'security-token', 'access-token', 'instance-url', 'jwt-file']
            },
            'jwt-file': {
                'dependencies': ['consumer-key', 'username'],
                'type': 'string',
                'excludes': ['password', 'security-token', 'access-token', 'instance-url', 'jwt-key']
            },
            'consumer-key': {
                'dependencies': ['username'],
                'type': 'string',
                'excludes': ['password', 'security-token', 'access-token', 'instance-url']
            }
        }
    }
}

def get_operation_schema(is_extract = True):
    return {
        'version': {
            'type': 'integer',
            'required': True,
            'allowed': [1]
        },
        'operation': {
            'type': 'list',
            'schema': {
                'type': 'dict',
                'schema': {
                    'sobject': {
                        'type': 'string',
                        'required': True
                    },
                    'file': {
                        'type': 'string',
                        'default_setter': lambda doc: doc['sobject'] + '.csv'
                    },
                    'result-file': {
                        'type': 'string',
                        'default_setter': lambda doc: doc['sobject'] + '-results.csv'
                    },
                    'input-validation': {
                        'type': 'string',
                        'default': 'default',
                        'allowed': ['none', 'default', 'strict']
                    },
                    'outside-lookup-behavior': {
                        'type': 'string',
                        'allowed': amaxa.OutsideLookupBehavior.all_values(),
                        'default': 'include'
                    },
                    'self-lookup-behavior': {
                        'type': 'string',
                        'allowed': amaxa.SelfLookupBehavior.all_values(),
                        'default': 'trace-all'
                    },
                    'extract': {
                        'type': 'dict',
                        'required': is_extract,
                        'schema': {
                            'all': {
                                'type': 'boolean',
                                'allowed': [True],
                                'excludes': ['descendents', 'query', 'ids']
                            },
                            'descendents': {
                                'type': 'boolean',
                                'allowed': [True],
                                'excludes': ['all', 'query', 'ids']
                            },
                            'query': {
                                'type': 'string',
                                'excludes': ['all', 'descendents', 'ids']
                            },
                            'ids': {
                                'type': 'list',
                                'excludes': ['all', 'descendents', 'query'],
                                'schema': {
                                    'type': 'string'
                                }
                            }
                        }
                    },
                    'field-group': {
                        'type': 'string',
                        'allowed': ['readable', 'writeable', 'smart'] if is_extract else ['writeable', 'smart'],
                        'excludes': ['fields']
                    },
                    'fields': {
                        'type': 'list',
                        'excludes': ['field-group'],
                        'schema': {
                            'type': ['string', 'dict'],
                            'schema': {
                                'field': {
                                    'type': 'string',
                                    'required': True
                                },
                                'column': {
                                    'type': 'string',
                                    'required': False
                                },
                                'transforms': {
                                    'type': 'list',
                                    'schema': {
                                        'type': 'string',
                                        'allowed': transforms.__all__
                                    },
                                    'required': False
                                },
                                'outside-lookup-behavior': {
                                    'type': 'string',
                                    'allowed': amaxa.OutsideLookupBehavior.all_values()
                                },
                                'self-lookup-behavior': {
                                    'type': 'string',
                                    'allowed': amaxa.SelfLookupBehavior.all_values()
                                }
                            }
                        }
                    }
                }
            }
        }
    }
