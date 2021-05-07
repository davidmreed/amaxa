import csv

from .. import amaxa
from .core import OperationLoader
from .input_type import InputType


class ExtractionOperationLoader(OperationLoader):
    def __init__(self, in_dict, connection):
        super().__init__(in_dict, connection, InputType.EXTRACT_OPERATION)

    def _validate(self):
        self._validate_sobjects("queryable")
        self._validate_field_mapping()

    def _initialize(self):
        super()._initialize()
        self._open_files()

    def _load(self):
        # Create the core operation
        self.result = amaxa.ExtractOperation(self.connection)

        options = self.input.get("options") or {}

        # Create the steps and data mappers
        for entry in self.input["operation"]:
            sobject = entry["sobject"]

            mapper = self._get_data_mapper(entry, "field", "column")
            if mapper is not None:
                self.result.mappers[sobject] = mapper

            # Determine the type of extraction
            query = None
            to_extract = entry.get("extract")

            if "ids" in to_extract:
                # Register the required IDs in the context
                try:
                    for record_id in to_extract.get("ids"):
                        self.result.add_dependency(
                            sobject, amaxa.SalesforceId(record_id)
                        )
                except ValueError:
                    self.errors.append(
                        "One or more invalid Id values provided for sObject {}".format(
                            sobject
                        )
                    )

                scope = amaxa.ExtractionScope.SELECTED_RECORDS
            elif "query" in to_extract:
                query = to_extract["query"]
                scope = amaxa.ExtractionScope.QUERY
            elif "all" in to_extract:
                scope = amaxa.ExtractionScope.ALL_RECORDS
            else:
                scope = amaxa.ExtractionScope.DESCENDENTS

            field_scope = self._get_field_scope(entry)
            field_scope.add("Id")

            # Options dictionary
            step_opts = options.copy()
            step_opts.update(entry.get("options", {}))

            step = amaxa.ExtractionStep(
                sobject,
                scope,
                field_scope,
                query,
                amaxa.SelfLookupBehavior.values_dict()[entry["self-lookup-behavior"]],
                amaxa.OutsideLookupBehavior.values_dict()[
                    entry["outside-lookup-behavior"]
                ],
                options=step_opts,
            )

            self._populate_lookup_behaviors(step, entry)
            self.result.add_step(step)

    def _post_load_validate(self):
        self._validate_field_permissions()

    def _post_initialize_validate(self):
        self._validate_lookup_behaviors()

    def _get_field_scope(self, entry):
        # Use the 'field-group', 'field', and 'exclude-fields' items to derive the field scope

        fields = set()

        if "field-group" in entry:
            # Don't include types we don't process: geolocations, addresses, and base64 fields.
            if entry["field-group"] in ["readable", "smart"]:

                def include(f):
                    return f["type"] not in ["location", "address", "base64"]

            else:

                def include(f):
                    return f["createable"] and f["type"] not in [
                        "location",
                        "address",
                        "base64",
                    ]

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

    def _open_files(self):
        # Open all of the output files
        # Create DictWriters and populate them in the context
        for (step, entry) in zip(self.result.steps, self.input["operation"]):
            try:
                file_handle = open(entry["file"], "w", newline="", encoding="utf-8")
                if step.sobjectname not in self.result.mappers:
                    fieldnames = step.field_scope
                else:
                    fieldnames = [
                        self.result.mappers[step.sobjectname].transform_key(k)
                        for k in step.field_scope
                    ]

                output = csv.DictWriter(
                    file_handle,
                    fieldnames=sorted(
                        fieldnames, key=lambda x: x if x != "Id" else " Id"
                    ),
                    extrasaction="ignore",
                )
                output.writeheader()
                self.result.file_store.set_file(
                    step.sobjectname, amaxa.FileType.OUTPUT, file_handle
                )
                self.result.file_store.set_csv(
                    step.sobjectname, amaxa.FileType.OUTPUT, output
                )
            except IOError as exp:
                self.errors.append(
                    "Unable to open file {} for writing ({}).".format(
                        entry["file"], exp
                    )
                )
