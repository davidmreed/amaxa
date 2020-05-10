import csv

from .. import amaxa, constants, mapper_cache
from .core import OperationLoader
from .input_type import InputType


class LoadOperationLoader(OperationLoader):
    def __init__(self, in_dict, connection, use_state=False):
        super().__init__(in_dict, connection, InputType.LOAD_OPERATION)
        self.use_state = use_state

    def _validate(self):
        self._validate_sobjects("createable")
        self._validate_field_mapping()
        self._validate_caching_schema()

    def _load(self):
        # Create the core operation
        self.result = amaxa.LoadOperation(self.connection)
        self.result.mapper_cache = mapper_cache.ObjectMapperCache()

        # Collect options
        options = self.input.get("options") or {}

        # Register mapped sObjects
        mapped_schema = self.input.get("object-mappings", [])
        for mapping in mapped_schema:
            sobject = mapping["sobject"]
            key_fields = mapping["key-fields"]
            self.result.mapper_cache.add_cached_sobject(sobject, key_fields)

        # Create the steps and data mappers
        for entry in self.input["operation"]:
            sobject = entry["sobject"]

            # Mapper
            mapper = self._get_data_mapper(entry, "column", "field")
            if mapper is not None:
                self.result.mappers[sobject] = mapper

            # Field scope
            field_scope = self._get_field_scope(entry)
            # Options dictionary
            step_opts = options.copy()
            step_opts.update(entry.get("options", {}))

            step = amaxa.LoadStep(
                sobject,
                field_scope,
                amaxa.OutsideLookupBehavior.values_dict()[
                    entry["outside-lookup-behavior"]
                ],
                options=step_opts,
            )

            self._populate_lookup_behaviors(step, entry)
            self.result.add_step(step)

        self._add_reference_mappers()

    def _add_reference_mappers(self):
        mapped_sobjects = self.result.mapper_cache.get_cached_sobjects()
        if not mapped_sobjects:
            return

        for step in self.result.steps:
            # Identify relationships to mapped objects and add transformers
            field_map = self.result.get_filtered_field_map(
                step.sobjectname,
                lambda f: any([sobj in mapped_sobjects for sobj in f["referenceTo"]]),
            )

            for field in step.field_scope:
                if field in field_map:
                    # This is a lookup field that refers to at least one mapped sObject
                    mapper = self.result.mappers.setdefault(
                        step.sobjectname, amaxa.DataMapper()
                    )
                    mapper.field_transforms.setdefault(field, []).append(
                        self.result.mapper_cache.get_reference_transformer(
                            [
                                sobject["keyPrefix"]
                                for sobject in self.connection.get_global_describe()[
                                    "sobjects"
                                ]
                                if sobject["name"] in mapped_sobjects
                            ],
                            amaxa.MappingMissBehavior.ERROR,  # TODO: use the specified behavior
                        ),
                    )

    def _post_load_validate(self):
        self._validate_field_permissions("createable")

    def _initialize(self):
        super()._initialize()
        self._open_files()

    def _get_field_scope(self, entry):
        # Use the 'field-group', 'field', and 'exclude-fields' items to derive the field scope

        fields = set()

        if "field-group" in entry:
            # Validation clamps acceptable values to 'writeable' or 'smart' by this point.
            # Don't include types we don't process: geolocations, addresses, and base64 fields.
            # Geolocations and addresses are omitted automatically (they aren't writeable)
            def include(f):
                return f["createable"] and f["type"] != "base64"

            fields.update(
                self.result.get_filtered_field_map(entry["sobject"], include).keys()
            )

        if "fields" in entry:
            fields.update(
                {f if isinstance(f, str) else f["field"] for f in entry["fields"]}
            )

        if "exclude-fields" in entry:
            for f in entry["exclude-fields"]:
                fields.discard(f)

        return fields

    def _open_input_file(self, filename, sobject):
        try:
            file_handle = open(filename, "r")
            input_file = csv.DictReader(file_handle)
            self.result.file_store.set_file(sobject, amaxa.FileType.INPUT, file_handle)
            self.result.file_store.set_csv(sobject, amaxa.FileType.INPUT, input_file)
        except IOError as exp:
            self.errors.append(f"Unable to open file {filename} for reading ({exp}).")

    def _open_files(self):
        # Open all of the input and output files
        # Create DictReaders and populate them in the context

        # Mapped sObjects
        for entry in self.input.get("object-mappings", []):
            self._open_input_file(entry["file"], entry["sobject"])

        # Operation sObject
        for (step, entry) in zip(self.result.steps, self.input["operation"]):
            self._open_input_file(entry["file"], step.sobjectname)

            try:
                f = open(
                    entry["result-file"], "w" if not self.use_state else "a", newline=""
                )
                output = csv.DictWriter(
                    f,
                    fieldnames=[
                        constants.ORIGINAL_ID,
                        constants.NEW_ID,
                        constants.ERROR,
                    ],
                )
                if not self.use_state:
                    output.writeheader()

                self.result.file_store.set_file(
                    step.sobjectname, amaxa.FileType.RESULT, f
                )
                self.result.file_store.set_csv(
                    step.sobjectname, amaxa.FileType.RESULT, output
                )
            except IOError as exp:
                self.errors.append(
                    "Unable to open file {} for writing ({})".format(
                        entry["result-file"], exp
                    )
                )

    def _post_initialize_validate(self):
        self._validate_dependent_field_permissions()
        self._validate_lookup_behaviors()
        self._validate_input_file_columns()
        self._validate_mapping_file_columns()

    def _validate_dependent_field_permissions(self):
        """Validate that dependent lookups are updateable."""
        for step in self.result.steps:
            field_map = self.result.get_field_map(step.sobjectname)
            for f in step.dependent_lookups | step.self_lookups:
                if not field_map[f]["updateable"]:
                    self.errors.append(
                        "Field {}.{} is a dependent lookup, but is not updateable.".format(
                            step.sobjectname, f
                        )
                    )

    def _validate_mapping_file_columns(self):
        """Validate the column sets in sObject mapping files."""

        for entry in self.input.get("object-mappings", []):
            input_file = self.result.file_store.get_csv(
                entry["sobject"], amaxa.FileType.INPUT
            )

            file_field_set = set(input_file.fieldnames)
            if "Id" in file_field_set:
                file_field_set.remove("Id")

            required_set = set(entry["key-fields"])

            if required_set != file_field_set:
                self.errors.append(
                    f"Mapping file for sObject {entry['sobject']} does not contain the correct column set for this mapping."
                )

    def _validate_input_file_columns(self):
        """Validate the column sets in the input files.
        For each file, if validation is active, check as follows.
        For field group steps, validate that each column in the input file
        is mapped to a field within the field group, but allow "missing" columns.
        For explicit field list steps, require that the mapped column set and field scope be 1:1"""

        for (step, entry) in zip(self.result.steps, self.input["operation"]):
            if entry["input-validation"] == "none":
                continue

            input_file = self.result.file_store.get_csv(
                step.sobjectname, amaxa.FileType.INPUT
            )
            file_field_set = set(input_file.fieldnames)
            if "Id" in file_field_set:
                file_field_set.remove("Id")

            # If we have transforms in place, transform all the column names into field names.
            if step.sobjectname in self.result.mappers:
                file_field_set = {
                    self.result.mappers[step.sobjectname].transform_key(f)
                    for f in file_field_set
                }

            if "field-group" in entry and entry["input-validation"] == "default":
                # Field group validation: file can omit columns but can't have extra
                # For the 'smart' field group, we permit any readable (but not writeable) fields
                # to be in the file, since the file was likely pulled with 'smart'=='readable'
                if entry["field-group"] == "smart":
                    comparand = set(
                        self.result.get_filtered_field_map(
                            step.sobjectname,
                            lambda f: f["type"]
                            not in ["location", "address", "base64"],
                        ).keys()
                    )
                else:
                    comparand = step.field_scope

                if not comparand.issuperset(file_field_set):
                    self.errors.append(
                        "Input file for sObject {} contains excess columns over field group '{}': {}".format(
                            step.sobjectname,
                            entry["field-group"],
                            ", ".join(sorted(file_field_set.difference(comparand))),
                        )
                    )
            else:
                # Field scope validation, or strict-mode group validation.
                # File columns must match field scope precisely.
                if step.field_scope != file_field_set:
                    self.errors.append(
                        "Input file for sObject {} does not match specified field scope.\nScope: {}\nFile Columns: {}\n".format(
                            step.sobjectname,
                            ", ".join(sorted(step.field_scope)),
                            ", ".join(sorted(file_field_set)),
                        )
                    )

    def _validate_caching_schema(self):
        pass  # TODO: implement
