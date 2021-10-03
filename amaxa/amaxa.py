import abc
import csv
import functools
import logging
from enum import Enum, unique

from . import constants


@unique
class StringEnum(Enum):
    @classmethod
    def all_values(cls):
        return [m.value for m in cls]

    @classmethod
    def values_dict(cls):
        return {m.value: m for m in cls}


class ExtractionScope(StringEnum):
    ALL_RECORDS = "all"
    QUERY = "query"
    SELECTED_RECORDS = "some"
    DESCENDENTS = "children"


class SelfLookupBehavior(StringEnum):
    TRACE_ALL = "trace-all"
    TRACE_NONE = "trace-none"


class OutsideLookupBehavior(StringEnum):
    DROP_FIELD = "drop-field"
    INCLUDE = "include"
    ERROR = "error"


class LoadStage(StringEnum):
    INSERTS = "inserts"
    DEPENDENTS = "dependents"


class FileType(Enum):
    INPUT = 1
    OUTPUT = 2
    RESULT = 3
    STATE = 4


class AmaxaException(Exception):
    pass


class SalesforceId(object):
    def __init__(self, idstr):
        if isinstance(idstr, SalesforceId):
            self.id = idstr.id
        else:
            idstr = idstr.strip()
            if len(idstr) == 15:
                suffix = ""
                for i in range(0, 3):
                    baseTwo = 0
                    for j in range(0, 5):
                        character = idstr[i * 5 + j]
                        if character >= "A" and character <= "Z":
                            baseTwo += 1 << j
                    suffix += "ABCDEFGHIJKLMNOPQRSTUVWXYZ012345"[baseTwo]
                self.id = idstr + suffix
            elif len(idstr) == 18:
                self.id = idstr
            else:
                raise ValueError("Salesforce Ids must be 15 or 18 characters.")

    def __eq__(self, other):
        if isinstance(other, SalesforceId):
            return self.id == other.id
        elif isinstance(other, str):
            return self.id == SalesforceId(other).id

        return False

    def __hash__(self):
        return hash(self.id)

    def __str__(self):
        return self.id

    def __repr__(self):
        return self.id


class FileStore(object):
    def __init__(self):
        self.store = {}
        self.csv_store = {}

    def set_file(self, sobject, ftype, f):
        self.store[(sobject, ftype)] = f

    def set_csv(self, sobject, ftype, f):
        self.csv_store[(sobject, ftype)] = f

    def get_file(self, sobject, ftype):
        return self.store[(sobject, ftype)]

    def get_csv(self, sobject, ftype):
        return self.csv_store[(sobject, ftype)]

    def close(self):
        for f in self.store.values():
            f.close()


class Operation(metaclass=abc.ABCMeta):
    def __init__(self, connection):
        self.steps = []
        self.connection = connection
        self._bulk = None
        self.logger = logging.getLogger("amaxa")
        self.file_store = FileStore()

    def run(self):
        try:
            self.initialize()
            return self.execute()
        except Exception as e:
            self.logger.error("Unexpected exception {} occurred.".format(str(e)))
            return -1
        finally:
            self.file_store.close()

    def initialize(self):
        for s in self.steps:
            s.initialize()

    @abc.abstractmethod
    def execute(self):
        pass

    def add_step(self, step):
        step.context = self
        self.steps.append(step)

    def get_sobject_list(self):
        return [step.sobjectname for step in self.steps]

    def get_sobject_name_for_id(self, id):
        return self.connection.get_sobject_name_for_id(id)

    def get_describe(self, sobjectname):
        return self.connection.get_sobject_describe(sobjectname)

    def get_field_map(self, sobjectname):
        return self.connection.get_sobject_field_map(sobjectname)

    def get_filtered_field_map(self, sobjectname, lam):
        field_map = self.get_field_map(sobjectname)

        return {k: field_map[k] for k in field_map if lam(field_map[k])}


class Step(metaclass=abc.ABCMeta):
    def __init__(self, sobjectname, field_scope):
        self.sobjectname = sobjectname
        self.field_scope = field_scope
        self.context = None
        self.options = {}

    def get_option(self, opt):
        return self.options.get(opt) or constants.OPTION_DEFAULTS[opt]

    def get_field_list(self):
        return ", ".join(self.field_scope)

    def initialize(self):
        # Determine whether we have any self-lookups or dependent lookups
        field_map = self.context.get_field_map(self.sobjectname)
        sobjects = self.context.get_sobject_list()

        # Filter for lookup fields that have at least one referent that is part of
        # this extraction. Users will see a warning for other lookups (on load),
        # and we'll just treat them like normal exported non-lookup fields
        self.all_lookups = {
            f
            for f in self.field_scope
            if field_map[f]["type"] == "reference"
            and any([s in sobjects for s in field_map[f]["referenceTo"]])
        }

        # Filter for lookup fields that are self-lookups
        # At present, we are assuming that there are no polymorphic self-lookup fields
        # in Salesforce. Should these exist, we'd have potential issues.
        self.self_lookups = {
            f
            for f in self.all_lookups
            if self.sobjectname in field_map[f]["referenceTo"]
        }

        # Filter for descendent lookups - fields that lookup to an object above this one
        # in the extraction and can be used to identify descendent records of *this* object
        self.descendent_lookups = {
            f
            for f in self.all_lookups
            if any(
                [
                    sobjects.index(refTo) < sobjects.index(self.sobjectname)
                    for refTo in field_map[f]["referenceTo"]
                    if refTo in sobjects
                ]
            )
        }

        # Filter for dependent lookups - fields that look up to an object
        # below this one in the extraction. These fields automatically have
        # dependencies registered when they're extracted.

        # A (polymorphic) field may be both a descendent lookup (up the hierarchy)
        # and a dependent lookup (down the hierarchy), as well as a lookup
        # to some arbitrary object outside the hierarchy.
        self.dependent_lookups = {
            f
            for f in self.all_lookups
            if any(
                [
                    sobjects.index(refTo) > sobjects.index(self.sobjectname)
                    for refTo in field_map[f]["referenceTo"]
                    if refTo in sobjects
                ]
            )
        }

    @abc.abstractmethod
    def execute(self):
        pass


class LoadOperation(Operation):
    def __init__(self, connection):
        super().__init__(connection)
        self.mappers = {}
        self.global_id_map = {}
        self.success = True
        self.stage = LoadStage.INSERTS

    def register_new_id(self, sobjectname, old_id, new_id):
        self.global_id_map[old_id] = new_id
        self.file_store.get_csv(sobjectname, FileType.RESULT).writerow(
            {constants.ORIGINAL_ID: str(old_id), constants.NEW_ID: str(new_id)}
        )

    def register_error(self, sobjectname, old_id, error):
        self.file_store.get_csv(sobjectname, FileType.RESULT).writerow(
            {constants.ORIGINAL_ID: str(old_id), constants.ERROR: error}
        )
        self.success = False

    def get_new_id(self, old_id):
        return self.global_id_map.get(old_id, None)

    def execute(self):
        self.logger.info(
            "Starting load with sObjects %s", ", ".join(self.get_sobject_list())
        )
        if self.stage is LoadStage.INSERTS:
            for s in self.steps:
                self.logger.info("%s: starting load", s.sobjectname)
                s.execute()

                # After each step, check whether errors happened and stop the process.
                if not self.success:
                    self.logger.error(
                        "%s: errors took place during load. See results file for details.",
                        s.sobjectname,
                    )
                    return -1

            self.stage = LoadStage.DEPENDENTS

        if self.stage is LoadStage.DEPENDENTS:
            for s in self.steps:
                self.logger.info(
                    "%s: populating dependent and self-lookups", s.sobjectname
                )
                s.execute_dependent_updates()

                if not self.success:
                    self.logger.error(
                        "%s: errors took place during dependent updates. See results file for details.",
                        s.sobjectname,
                    )
                    return -1

        return 0


class LoadStep(Step):
    def __init__(
        self,
        sobjectname,
        field_scope,
        outside_lookup_behavior=OutsideLookupBehavior.INCLUDE,
        options=None,
    ):
        self.sobjectname = sobjectname
        self.field_scope = field_scope
        self.outside_lookup_behavior = outside_lookup_behavior
        self.lookup_behaviors = {}
        self.dependent_lookup_records = []
        self.options = options or {}

        self.context = None

    def set_lookup_behavior_for_field(self, field, behavior):
        self.lookup_behaviors[field] = behavior

    def get_lookup_behavior_for_field(self, field):
        return self.lookup_behaviors.get(field, self.outside_lookup_behavior)

    def get_value_for_lookup(self, lookup, value, record_id):
        if value == "":
            return ""

        b = self.get_lookup_behavior_for_field(lookup)

        mapped_id = self.context.get_new_id(SalesforceId(value))

        if mapped_id is not None:
            return str(mapped_id)
        elif b is OutsideLookupBehavior.INCLUDE:
            return value
        elif b is OutsideLookupBehavior.ERROR:
            raise AmaxaException(
                f"{self.sobjectname} {record_id} has an outside reference in field {lookup} ({value}), "
                "which is not allowed by the extraction configuration.",
            )
        elif b is OutsideLookupBehavior.DROP_FIELD:
            return ""

    def populate_lookups(self, record, lookups, id):
        return {
            k: record[k]
            if k not in lookups
            else self.get_value_for_lookup(k, record[k], id)
            for k in record
        }

    def primitivize(self, record):
        # We're using the Bulk API over JSON, so values can be specified as strings (not converted to JSON primitives)
        # We will apply a light transformation to ensure we format correctly and respect a few Boolean equivalents
        def convert_value(value, field_type):
            if field_type == "xsd:boolean":
                if value is None or value.lower() in ["no", "false", "n", "f", "0", ""]:
                    return "false"
                elif value.lower() in ["yes", "true", "y", "t", "1"]:
                    return "true"
                raise ValueError(f"Invalid Boolean value {value}")
            elif value is None or len(value) == 0:
                return None
            elif field_type == "tns:ID":
                return str(value)
            elif field_type in [
                "xsd:string",
                "xsd:date",
                "xsd:dateTime",
                "xsd:int",
                "xsd:double",
            ]:
                return value

            return None

        field_map = self.context.get_field_map(self.sobjectname)
        return {k: convert_value(record[k], field_map[k]["soapType"]) for k in record}

    def transform_record(self, record):
        if self.sobjectname in self.context.mappers:
            record = self.context.mappers[self.sobjectname].transform_record(record)

        return {k: record[k] for k in record if k in self.field_scope}

    def clean_dependent_lookups(self, record):
        all_lookups = self.dependent_lookups | self.self_lookups

        return {k: record[k] for k in record if k not in all_lookups}

    def extract_dependent_lookups(self, record):
        all_lookups = self.dependent_lookups | self.self_lookups

        return {k: record[k] for k in record if k in all_lookups or k == "Id"}

    def execute(self):
        # Read our incoming file.
        # Apply transformations specified in our configuration file (column name -> field name, for example)
        # Then, populate all direct lookups. Dependent lookups and self-lookups will be populated in a later pass.
        records_to_load = []
        original_ids = []
        success = True

        reader = self.context.file_store.get_csv(self.sobjectname, FileType.INPUT)
        for record in reader:
            # We might have resumed this operation. Check to be sure this record hasn't been loaded already.
            if self.context.get_new_id(SalesforceId(record["Id"])) is not None:
                continue

            # We need to save off the original record Id because it'll be cleaned from the record before insert.
            # We use the original Id for error reporting.
            original_ids.append(record["Id"])

            # Then, prep this record for the Bulk API, populate its lookups, apply transforms, and clean dependent lookups
            try:
                record = self.primitivize(
                    self.populate_lookups(
                        self.clean_dependent_lookups(self.transform_record(record)),
                        self.descendent_lookups,
                        original_ids[-1],
                    )
                )
                records_to_load.append(record)
            except AmaxaException as e:
                self.context.register_error(self.sobjectname, original_ids[-1], str(e))
                success = False
            except ValueError as e:
                self.context.register_error(
                    self.sobjectname,
                    original_ids[-1],
                    f"Bad data in record {original_ids[-1]}: {str(e)}",
                )
                success = False

        if not success or len(records_to_load) == 0:
            return

        for i, r in enumerate(
            self.context.connection.bulk_api_insert(
                self.sobjectname,
                records_to_load,
                self.get_option("bulk-api-timeout"),
                self.get_option("bulk-api-poll-interval"),
                self.get_option("bulk-api-batch-size"),
                self.get_option("bulk-api-mode"),
            )
        ):
            if r.success:
                self.context.register_new_id(
                    self.sobjectname,
                    SalesforceId(original_ids[i]),
                    SalesforceId(r.id),  # note lowercase in result
                )
            else:
                self.context.register_error(
                    self.sobjectname, original_ids[i], self.format_error(r.error)
                )

    def execute_dependent_updates(self):
        # Populate dependent and self-lookups in a single pass
        records_to_load = []
        original_ids = []
        all_lookups = self.dependent_lookups | self.self_lookups
        success = True

        if len(all_lookups) > 0:
            # Re-check, for each record, whether we have any loading to do.
            # If all of the dependent lookups prove to be dropped outside references,
            # we have no work to do.
            self.reset_input_csv()
            reader = self.context.file_store.get_csv(self.sobjectname, FileType.INPUT)
            for record in reader:
                try:
                    cleaned_record = self.populate_lookups(
                        self.extract_dependent_lookups(record),
                        all_lookups,
                        record["Id"],
                    )
                    if (
                        len(
                            list(
                                filter(
                                    lambda r: r is not None and r != "",
                                    cleaned_record.values(),
                                )
                            )
                        )
                        > 1
                    ):  # 1 for the Id
                        # Populate the new Id for this record
                        original_ids.append(cleaned_record["Id"])
                        cleaned_record["Id"] = str(
                            self.context.get_new_id(SalesforceId(cleaned_record["Id"]))
                        )
                        records_to_load.append(cleaned_record)
                except AmaxaException as e:
                    self.context.register_error(self.sobjectname, record["Id"], str(e))
                    success = False

            if success and len(records_to_load) > 0:
                for i, r in enumerate(
                    self.context.connection.bulk_api_update(
                        self.sobjectname,
                        records_to_load,
                        self.get_option("bulk-api-timeout"),
                        self.get_option("bulk-api-poll-interval"),
                        self.get_option("bulk-api-batch-size"),
                        self.get_option("bulk-api-mode"),
                    )
                ):
                    if not r.success:
                        self.context.register_error(
                            self.sobjectname,
                            original_ids[i],
                            self.format_error(r.error),
                        )

    def format_error(self, error):
        return "\n".join(
            [
                "{}: {}{}{}".format(
                    e["statusCode"],
                    e["message"],
                    " ({}).".format(", ".join(e["fields"]))
                    if len(e["fields"]) > 0
                    else "",
                    " " + e["extendedErrorDetails"]
                    if e["extendedErrorDetails"] is not None
                    else "",
                )
                for e in error
            ]
        )

    def reset_input_csv(self):
        fh = self.context.file_store.get_file(self.sobjectname, FileType.INPUT)
        fh.seek(0)
        self.context.file_store.set_csv(
            self.sobjectname, FileType.INPUT, csv.DictReader(fh)
        )


class ExtractOperation(Operation):
    def __init__(self, connection):
        super().__init__(connection)
        self.required_ids = {}
        self.extracted_ids = {}
        self.mappers = {}

    def execute(self):
        self.logger.info(
            "Starting extraction with sObjects %s", self.get_sobject_list()
        )
        for s in self.steps:
            self.logger.info("%s: starting extraction", s.sobjectname)
            s.execute()
            if len(s.errors) > 0:
                self.logger.error(
                    "%s: errors took place during extraction:\n%s",
                    s.sobjectname,
                    "\n".join(s.errors),
                )
                return -1
            else:
                self.logger.info(
                    "%s: extracted %d record%s",
                    s.sobjectname,
                    len(self.get_extracted_ids(s.sobjectname)),
                    "s" if len(self.get_extracted_ids(s.sobjectname)) != 1 else "",
                )

        return 0

    def add_dependency(self, sobjectname, id):
        if sobjectname not in self.required_ids:
            self.required_ids[sobjectname] = set()
        if id not in self.get_extracted_ids(sobjectname):
            self.required_ids[sobjectname].add(id)

    def get_dependencies(self, sobjectname):
        return (
            self.required_ids[sobjectname]
            if sobjectname in self.required_ids
            else set()
        )

    def get_sobject_ids_for_reference(self, sobjectname, field):
        ids = set()
        for name in self.get_field_map(sobjectname)[field]["referenceTo"]:
            # For each sObject that we've extracted data for,
            # if that object is a potential reference target for this field,
            # accumulate those Ids in a Set.
            if name in self.extracted_ids:
                ids |= self.extracted_ids[name]

        return ids

    def get_extracted_ids(self, sobjectname):
        return (
            self.extracted_ids[sobjectname]
            if sobjectname in self.extracted_ids
            else set()
        )

    def store_result(self, sobjectname, record):
        if sobjectname not in self.extracted_ids:
            self.extracted_ids[sobjectname] = set()

        if SalesforceId(record["Id"]) not in self.extracted_ids[sobjectname]:
            self.logger.debug(
                "%s: extracting record %s", sobjectname, SalesforceId(record["Id"])
            )
            self.extracted_ids[sobjectname].add(SalesforceId(record["Id"]))
            self.file_store.get_csv(sobjectname, FileType.OUTPUT).writerow(
                self.mappers[sobjectname].transform_record(record)
                if sobjectname in self.mappers
                else record
            )

        if (
            sobjectname in self.required_ids
            and SalesforceId(record["Id"]) in self.required_ids[sobjectname]
        ):
            self.required_ids[sobjectname].remove(SalesforceId(record["Id"]))


class ExtractionStep(Step):
    def __init__(
        self,
        sobjectname,
        scope,
        field_scope,
        where_clause=None,
        self_lookup_behavior=SelfLookupBehavior.TRACE_ALL,
        outside_lookup_behavior=OutsideLookupBehavior.INCLUDE,
        options=None,
    ):
        super().__init__(sobjectname, field_scope)
        self.scope = scope
        self.where_clause = where_clause
        self.self_lookup_behavior = self_lookup_behavior
        self.outside_lookup_behavior = outside_lookup_behavior
        self.lookup_behaviors = {}
        self.errors = []
        self.options = options or {}

    def set_lookup_behavior_for_field(self, f, behavior):
        self.lookup_behaviors[f] = behavior

    def get_self_lookup_behavior_for_field(self, f):
        return self.lookup_behaviors.get(f, self.self_lookup_behavior)

    def get_outside_lookup_behavior_for_field(self, f):
        return self.lookup_behaviors.get(f, self.outside_lookup_behavior)

    def execute(self):
        # If scope if ALL_RECORDS, execute a Bulk API job to extract all records
        # If scope is QUERY, execute a Bulk API job to download a query with where_clause
        # If scope is DESCENDENTS, pull based on objects that look up to any already
        # extracted object.
        # If scope is SELECTED_RECORDS, and if `context` has any registered dependencies,
        # perform a query to extract those records by Id.

        if self.scope == ExtractionScope.ALL_RECORDS:
            query = "SELECT {} FROM {}".format(self.get_field_list(), self.sobjectname)

            self.context.logger.debug(
                "%s: extracting all records using Bulk API query %s",
                self.sobjectname,
                query,
            )
            self.perform_bulk_api_pass(query)
            return
        elif self.scope == ExtractionScope.QUERY:
            query = "SELECT {} FROM {} WHERE {}".format(
                self.get_field_list(), self.sobjectname, self.where_clause
            )

            self.context.logger.debug(
                "%s: extracting filtered records using Bulk API query %s",
                self.sobjectname,
                query,
            )
            self.perform_bulk_api_pass(query)
        elif self.scope == ExtractionScope.DESCENDENTS:
            self.context.logger.debug(
                "%s: extracting descendent records based on lookups %s",
                self.sobjectname,
                ", ".join(self.descendent_lookups),
            )

            for f in self.descendent_lookups:
                self.perform_lookup_pass(f)

        # Fall through to grab all dependencies registered with the context, or SELECTED_RECORDS
        # Note that if we're tracing self-lookups, the parent objects of all extracted records so far
        # will already be registered as dependencies.

        self.resolve_registered_dependencies()

        # If we have any self-lookups, we now need to iterate to handle them.
        if (
            len(self.self_lookups) > 0
            and self.self_lookup_behavior is SelfLookupBehavior.TRACE_ALL
            and self.scope != ExtractionScope.ALL_RECORDS
        ):
            # First we query up to the parents of objects we've already obtained (i.e. the targets of their lookups)
            # Then we query down to the children of all objects obtained.
            # Then we query parents and children again.
            # We repeat until we get back no new Ids, which indicates that all references have been resolved.

            # Note that the initial parent query is handled in the dependency pass above, so we start on children.

            self.context.logger.debug(
                "%s: recursing to trace self-lookups", self.sobjectname
            )

            while True:
                before_count = len(self.context.get_extracted_ids(self.sobjectname))

                # Children
                for lookup in self.self_lookups:
                    self.perform_lookup_pass(lookup)

                # Parents
                self.resolve_registered_dependencies()

                after_count = len(self.context.get_extracted_ids(self.sobjectname))

                if before_count == after_count:
                    break

    def store_result(self, result):
        # Examine the received data to determine whether we have any cross-hierarchy lookups
        # or down-hierarchy dependencies to register

        field_map = self.context.get_field_map(self.sobjectname)
        sobject_list = self.context.get_sobject_list()

        # Add a dependency for the reference in each self lookup of this record.
        for lookup in self.self_lookups:
            if (
                self.get_self_lookup_behavior_for_field(lookup)
                is not SelfLookupBehavior.TRACE_NONE
                and result[lookup] is not None
            ):
                self.context.add_dependency(
                    self.sobjectname, SalesforceId(result[lookup])
                )

        # Register any dependencies from dependent lookups
        # Note that a dependent lookup can *also* be a descendent lookup (e.g. Task.WhatId),
        # so we handle polymorphic lookups carefully
        for f in self.dependent_lookups:
            lookup_value = result[f]
            if lookup_value is not None:
                # Determine what sObject this Id looks up to
                # If this is a regular lookup, it's the target of the field, and is always dependent.
                # If this lookup is polymorphic, we have to determine it based on the Id itself,
                # and this value may actually be a cross-hierarchy reference or descendent reference.
                if len(field_map[f]["referenceTo"]) > 1:
                    target_sobject = self.context.get_sobject_name_for_id(lookup_value)

                    if target_sobject not in sobject_list:
                        continue  # Ignore references to objects not in our extraction.

                    # Determine if this is really a dependent connection, or if it's a descendent
                    # that should be handled below.
                    # The descendent code looks for cross-hierarchy references
                    if sobject_list.index(target_sobject) < sobject_list.index(
                        self.sobjectname
                    ):
                        continue

                    # Otherwise, fall through to add a dependency
                else:
                    target_sobject = field_map[f]["referenceTo"][0]

                self.context.add_dependency(target_sobject, SalesforceId(lookup_value))

        # Check for cross-hierarchy lookup values:
        # references to records above us in the extraction hierarchy, but that weren't extracted already.
        for f in self.descendent_lookups:
            lookup_value = result[f]

            if lookup_value is not None:
                if len(field_map[f]["referenceTo"]) == 1:
                    target_sobject = field_map[f]["referenceTo"][0]
                else:
                    target_sobject = self.context.get_sobject_name_for_id(lookup_value)

                if lookup_value not in self.context.get_extracted_ids(target_sobject):
                    # This is a cross-hierarchy reference
                    behavior = self.get_outside_lookup_behavior_for_field(f)

                    if behavior is OutsideLookupBehavior.DROP_FIELD:
                        del result[f]
                    elif behavior is OutsideLookupBehavior.INCLUDE:
                        continue
                    elif behavior is OutsideLookupBehavior.ERROR:
                        self.errors.append(
                            "{} {} has an outside reference in field {} ({}), which is not allowed by the extraction configuration.".format(
                                self.sobjectname, result["Id"], f, result[f]
                            )
                        )

        # Finally, call through to the context to store this result.
        self.context.store_result(self.sobjectname, result)

    def resolve_registered_dependencies(self):
        pre_deps = self.context.get_dependencies(self.sobjectname).copy()
        for r in self.context.connection.retrieve_records_by_id(
            self.sobjectname, pre_deps, self.field_scope
        ):
            self.store_result(r)

        missing = self.context.get_dependencies(self.sobjectname).intersection(pre_deps)
        if len(missing) > 0:
            self.errors.append(
                "Unable to resolve dependencies for sObject {}. The following Ids could not be found: {}".format(
                    self.sobjectname, ", ".join([str(i) for i in missing])
                )
            )

    def perform_bulk_api_pass(self, query):
        # The JSON Bulk API returns DateTime values as epoch seconds, instead of ISO 8601-format strings.
        # If we have DateTime fields in our field set, postprocess the result before we store it.
        date_time_fields = [
            f
            for f in self.field_scope
            if self.context.get_field_map(self.sobjectname)[f]["type"] == "datetime"
        ]

        for result in self.context.connection.bulk_api_query(
            self.sobjectname,
            query,
            date_time_fields,
            self.get_option("bulk-api-poll-interval"),
        ):
            self.store_result(result)

    def perform_lookup_pass(self, field):
        id_set = self.context.get_sobject_ids_for_reference(self.sobjectname, field)

        if id_set:
            for rec in self.context.connection.query_records_by_reference_field(
                self.sobjectname, self.get_field_list(), field, id_set
            ):
                self.store_result(rec)


class DataMapper(object):
    def __init__(self, field_name_mapping=None, field_transforms=None):
        self.field_name_mapping = field_name_mapping or {}
        self.field_transforms = field_transforms or {}

    def transform_record(self, record):
        return {
            self.transform_key(k): self.transform_value(k, record[k]) for k in record
        }

    def transform_key(self, k):
        return self.field_name_mapping.get(k, k)

    def transform_value(self, k, v):
        return functools.reduce(lambda x, f: f(x), self.field_transforms.get(k, []), v)
