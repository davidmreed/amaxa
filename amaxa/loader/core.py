import abc
import collections
import json
import logging

import cerberus
import simple_salesforce
import yaml

from .. import amaxa, transforms
from . import schemas


def load_file(file_data):
    if file_data.name.endswith("json"):
        return json.load(file_data)

    return yaml.safe_load(file_data)


class Loader(metaclass=abc.ABCMeta):
    def __init__(self, in_dict, input_type):
        self.input = in_dict
        self.input_type = input_type
        self.errors = []
        self.warnings = []
        self.result = None

    def _get_validator(self):
        return cerberus.Validator(
            schemas.get_schema(self.input_type, self.input["version"])
        )

    def _validate_schema(self):
        if "version" not in self.input:
            self.errors.append("No version number present in schema")
            return

        if self.input["version"] not in schemas.get_available_versions(self.input_type):
            self.errors.append(
                "Schema version for {} not present or unsupported".format(
                    self.input_type.value
                )
            )
            return

        # Validate schema.
        validator = self._get_validator()

        self.input = validator.validated(self.input)
        if validator.errors:
            self.errors.extend(
                ["{}: {}".format(k, validator.errors[k]) for k in validator.errors]
            )

    def load(self):
        steps = [
            self._validate_schema,
            self._validate,
            self._load,
            self._post_load_validate,
            self._initialize,
            self._post_initialize_validate,
        ]

        for step in steps:
            step()
            if self.errors:
                self.result = None
                return

    def _load(self):
        pass

    def _validate(self):
        pass

    def _initialize(self):
        pass

    def _post_load_validate(self):
        pass

    def _post_initialize_validate(self):
        pass


class OperationLoader(Loader):
    def __init__(self, in_dict, connection, op_type):
        super().__init__(in_dict, op_type)
        self.connection = connection

    def _validate_field_mapping(self):
        # Validate that fields are listed only once, and that no column
        # is mapped in duplicate.
        for entry in self.input["operation"]:
            if "fields" in entry:
                field_counter = collections.Counter(
                    [f if isinstance(f, str) else f["field"] for f in entry["fields"]]
                )
                column_counter = collections.Counter(
                    [
                        f["column"] if isinstance(f, dict) and "column" in f else None
                        for f in entry["fields"]
                    ]
                )

                duplicate_fields = list(
                    filter(lambda f: field_counter[f] > 1, field_counter.keys())
                )
                duplicate_columns = list(
                    filter(
                        lambda f: f is not None and column_counter[f] > 1,
                        field_counter.keys(),
                    )
                )

                if duplicate_fields:
                    self.errors.append(
                        "{}: One or more fields is specified "
                        "multiple times: {}".format(
                            entry["sobject"], ", ".join(duplicate_fields)
                        )
                    )
                if duplicate_columns:
                    self.errors.append(
                        "{}: One or more columns is specified "
                        "multiple times: {}".format(
                            entry["sobject"], ", ".join(duplicate_columns)
                        )
                    )

    def _get_data_mapper(self, entry, source, dest):
        # Create the data mapper, if needed
        if "fields" in entry and any(
            [
                isinstance(f, dict) and ("column" in f or "transforms" in f)
                for f in entry["fields"]
            ]
        ):
            mapper = amaxa.DataMapper()

            for f in entry["fields"]:
                if isinstance(f, dict) and ("column" in f or "transforms" in f):
                    if "column" in f:
                        mapper.field_name_mapping[f[source]] = f[dest]

                    if "transforms" in f:
                        try:
                            field_name = f[source] if source in f else f[dest]
                            mapper.field_transforms[field_name] = [
                                self._build_transform(entry["sobject"], f["field"], t)
                                for t in f["transforms"]
                            ]
                        except transforms.TransformException as e:
                            self.errors.append(
                                f"Unable to create transforms for field {entry['sobject']}.{f['field']}: {e}"
                            )

            return mapper

        return None

    def _build_transform(self, sobject_name, field_name, transform):
        transform_factory = transforms.get_all_transforms()[transform["name"]]

        return transform_factory.get_transform(
            self.connection.get_sobject_field_map(sobject_name)[field_name],
            transform["options"],
        )

    def _populate_lookup_behaviors(self, step, entry):
        if "fields" in entry and any(
            [
                isinstance(f, dict)
                and ("self-lookup-behavior" in f or "outside-lookup-behavior" in f)
                for f in entry["fields"]
            ]
        ):
            for f in entry["fields"]:
                if isinstance(f, dict) and (
                    "self-lookup-behavior" in f or "outside-lookup-behavior" in f
                ):
                    if "self-lookup-behavior" in f:
                        step.set_lookup_behavior_for_field(
                            f["field"],
                            amaxa.SelfLookupBehavior.values_dict()[
                                f["self-lookup-behavior"]
                            ],
                        )
                    if "outside-lookup-behavior" in f:
                        step.set_lookup_behavior_for_field(
                            f["field"],
                            amaxa.OutsideLookupBehavior.values_dict()[
                                f["outside-lookup-behavior"]
                            ],
                        )

    def _validate_field_permissions(self, permission=None):
        # Validate that all fields designated for steps are real, writeable,
        # and of a supported type.
        all_sobjects = [step.sobjectname for step in self.result.steps]

        for step in self.result.steps:
            field_map = self.result.get_field_map(step.sobjectname)
            for field in step.field_scope:
                if field not in field_map or (
                    permission is not None and not field_map[field][permission]
                ):
                    self.errors.append(
                        "Field {}.{} does not exist or does not "
                        "have the correct CRUD permission{}.".format(
                            step.sobjectname,
                            field,
                            " (" + permission + ")" if permission else "",
                        )
                    )
                elif field_map[field]["type"] == "reference":
                    # Ensure that the target objects of this reference
                    # are included in the operation. If not, show a warning.
                    if not any(
                        [ref in all_sobjects for ref in field_map[field]["referenceTo"]]
                    ):
                        logging.getLogger("amaxa").warning(
                            "Field %s.%s is a reference none of whose targets (%s) "
                            "are included in the operation. Reference handlers "
                            "will be inactive for references to non-included sObjects.",
                            step.sobjectname,
                            field,
                            ", ".join(field_map[field]["referenceTo"]),
                        )
                    elif not all(
                        [ref in all_sobjects for ref in field_map[field]["referenceTo"]]
                    ):
                        logging.getLogger("amaxa").debug(
                            "Field %s.%s is a reference whose targets (%s) "
                            "are not all included in the operation. Reference handlers "
                            "will be inactive for references to non-included sObjects.",
                            step.sobjectname,
                            field,
                            ", ".join(field_map[field]["referenceTo"]),
                        )
                elif field_map[field]["type"] in ["location", "address", "base64"]:
                    self.errors.append(
                        "Field {}.{} is of an unsupported type ({})".format(
                            step.sobjectname, field, field_map[field]["type"]
                        )
                    )

    def _validate_sobjects(self, permission):
        try:
            global_describe = {
                entry["name"]: entry
                for entry in self.connection.get_global_describe()["sobjects"]
            }
        except simple_salesforce.SalesforceAuthenticationFailed as e:
            self.errors.append("Unable to authenticate to Salesforce: {}".format(e))
            return

        for entry in self.input["operation"]:
            sobject = entry["sobject"]

            if (
                sobject not in global_describe
                or not global_describe[sobject][permission]
            ):
                self.errors.append(
                    "sObject {} does not exist or does not "
                    "have the correct permission ({})".format(sobject, permission)
                )

    def _validate_lookup_behaviors(self):
        # Validate that lookup behaviors are associated with lookups
        # of the correct type (outside or self)
        for step in self.result.steps:
            for f in step.lookup_behaviors:
                if (
                    f in step.dependent_lookups
                    and step.lookup_behaviors[f] not in amaxa.OutsideLookupBehavior
                ) or (
                    f in step.self_lookups
                    and step.lookup_behaviors[f] not in amaxa.SelfLookupBehavior
                ):
                    self.errors.append(
                        "Lookup behavior '{}' specified for field {}."
                        "{} is not valid for this lookup type.".format(
                            step.lookup_behaviors[f].value, step.sobjectname, f
                        )
                    )

    def _initialize(self):
        self.result.initialize()
