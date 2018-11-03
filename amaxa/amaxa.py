import functools
import simple_salesforce
import logging

class ExtractionScope(object):
    ALL_RECORDS = 'all'
    QUERY = 'query'
    SELECTED_RECORDS = 'some'
    DESCENDENTS = 'children'

class SelfLookupBehavior:
    TRACE_ALL = 'trace-all'
    TRACE_NONE = 'trace-none'

class OutsideReferenceBehavior:
    DROP_RECORD = 'drop-record'
    DROP_FIELD = 'drop-field'
    INCLUDE = 'include'
    RECURSE = 'recurse'
    ERROR = 'error'

class SalesforceId(object):
    def __init__(self, idstr):
        if isinstance(idstr, SalesforceId):
            self.id = idstr.id
        else:
            if len(idstr) == 15:
                suffix = ''
                for i in range(0, 3):
                    baseTwo = 0
                    for j in range (0, 5):
                        character = idstr[i*5+j]
                        if character >= 'A' and character <= 'Z':
                            baseTwo += 1 << j
                    suffix += 'ABCDEFGHIJKLMNOPQRSTUVWXYZ012345'[baseTwo]
                self.id = idstr + suffix
            elif len(idstr) == 18:
                self.id = idstr
            else:
                raise ValueError('Salesforce Ids must be 15 or 18 characters.')

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
        return 'Salesforce Id: ' + self.id

class OperationContext(object):
    def __init__(self, connection):
        self.steps = []
        self.connection = connection
        self.describe_info = {}
        self.field_maps = {}
        self.proxy_objects = {}
        self.bulk_proxy_objects = {}
        self.required_ids = {}
        self.extracted_ids = {}
        self.output_files = {}
        self.mappers = {}
        self.logger = logging.getLogger('amaxa')
    
    def add_step(self, step):
        step.context = self
        self.steps.append(step)

    def execute(self):
        self.logger.info('Starting extraction with sObjects %s', self.get_sobject_list())
        for s in self.steps:
            self.logger.info('Extracting %s', s.sobjectname)
            s.execute()

    def set_output_file(self, sobjectname, f):
        self.output_files[sobjectname] = f

    def add_dependency(self, sobjectname, id):
        if sobjectname not in self.required_ids:
            self.required_ids[sobjectname] = set()
        if id not in self.get_extracted_ids(sobjectname):
            self.required_ids[sobjectname].add(id)

    def get_dependencies(self, sobjectname):
        return self.required_ids[sobjectname] if sobjectname in self.required_ids else set()

    def get_proxy_object(self, sobjectname):
        if sobjectname not in self.proxy_objects:
            self.proxy_objects[sobjectname] = getattr(self.connection, sobjectname)

        return self.proxy_objects[sobjectname]

    def get_bulk_proxy_object(self, sobjectname):
        if sobjectname not in self.bulk_proxy_objects:
            self.bulk_proxy_objects[sobjectname] = getattr(self.connection.bulk, sobjectname)

        return self.bulk_proxy_objects[sobjectname]

    def get_describe(self, sobjectname):
        if sobjectname not in self.describe_info:
            self.describe_info[sobjectname] = self.get_proxy_object(sobjectname).describe()
            self.field_maps[sobjectname] = { f.get('name') : f for f in self.describe_info[sobjectname].get('fields') }

        return self.describe_info[sobjectname]

    def get_field_map(self, sobjectname):
        if sobjectname not in self.describe_info:
            self.get_describe(sobjectname)

        return self.field_maps[sobjectname]

    def get_filtered_field_map(self, sobjectname, lam):
        field_map = self.get_field_map(sobjectname)

        return { k: field_map[k] for k in field_map if lam(field_map[k]) }

    def get_sobject_ids_for_reference(self, sobjectname, field):
        ids = set()
        for name in self.get_field_map(sobjectname)[field]['referenceTo']:
            # For each sObject that we've extracted data for,
            # if that object is a potential reference target for this field,
            # accumulate those Ids in a Set.
            if name in self.extracted_ids:
                ids |= self.extracted_ids[name]

        return ids

    def get_extracted_ids(self, sobjectname):
        return self.extracted_ids[sobjectname] if sobjectname in self.extracted_ids else set()

    def store_result(self, sobjectname, record):
        if sobjectname not in self.extracted_ids:
            self.extracted_ids[sobjectname] = set()

        self.logger.debug('%s: extracting record %s', sobjectname, SalesforceId(record['Id']))
        self.extracted_ids[sobjectname].add(SalesforceId(record['Id']))
        self.output_files[sobjectname].writerow(
            self.mappers[sobjectname].transform_record(record) if sobjectname in self.mappers
            else record
        )

        if sobjectname in self.required_ids and SalesforceId(record['Id']) in self.required_ids[sobjectname]:
            self.required_ids[sobjectname].remove(SalesforceId(record['Id']))
    
    def get_sobject_list(self):
        return [step.sobjectname for step in self.steps]


class SingleObjectExtraction(object):
    def __init__(self, sobjectname, scope, field_scope, where_clause=None, self_lookup_behavior=SelfLookupBehavior.TRACE_ALL):
        self.sobjectname = sobjectname
        self.scope = scope
        self.field_scope = field_scope
        self.where_clause = where_clause
        self.self_lookup_behavior = self_lookup_behavior

        self.context = None

    def get_field_list(self):
        return ', '.join(self.field_scope)

    def scan_fields(self):
        # Determine whether we have any self-lookups or dependent lookups
        field_map = self.context.get_field_map(self.sobjectname)

        self.all_lookups = { f for f in self.field_scope if field_map[f]['type'] == 'reference' }
        self.self_lookups = { f for f in self.all_lookups if self.sobjectname in field_map[f]['referenceTo'] }
        self.dependent_lookups = { 
            f for f in self.all_lookups
            if f not in self.self_lookups and \
                field_map[f]['referenceTo'][0] in self.context.get_sobject_list() and \
                self.context.get_sobject_list().index(field_map[f]['referenceTo'][0]) \
                > self.context.get_sobject_list().index(self.sobjectname) }
        # FIXME: we may need to add some polymorphic lookup handling here.

    def execute(self):
        self.scan_fields()
        # If scope if ALL_RECORDS, execute a Bulk API job to extract all records
        # If scope is QUERY, execute a Bulk API job to download a query with where_clause
        # If scope is DESCENDENTS, pull based on objects that look up to any already
        # extracted object.
        # If scope is SELECTED_RECORDS, and if `context` has any registered dependencies,
        # perform a query to extract those records by Id.

        if self.scope == ExtractionScope.ALL_RECORDS:
            query = 'SELECT {} FROM {}'.format(self.get_field_list(), self.sobjectname)

            self.context.logger.debug('%s: extracting all records using Bulk API query %s', self.sobjectname, query)
            self.perform_bulk_api_pass(query)
        elif self.scope == ExtractionScope.QUERY:
            query = 'SELECT {} FROM {} WHERE {}'.format(self.get_field_list(), self.sobjectname, self.where_clause)

            self.context.logger.debug('%s: extracting filtered records using Bulk API query %s', self.sobjectname, query)
            self.perform_bulk_api_pass(query)
        elif self.scope == ExtractionScope.DESCENDENTS:
            lookups = self.context.get_filtered_field_map(self.sobjectname, lambda f: f['type'] == 'reference')

            self.context.logger.debug('%s: extracting descendent records based on lookups %s', self.sobjectname, ', '.join(lookups))

            for f in self.field_scope:
                if f in lookups:
                    self.perform_lookup_pass(f)

        # Fall through to grab all dependencies registered with the context, or SELECTED_RECORDS
        # Note that if we're tracing self-lookups, the parent objects of all extracted records so far
        # will already be registered as dependencies.

        self.resolve_registered_dependencies()

        # If we have any self-lookups, we now need to iterate to handle them.
        if len(self.self_lookups) > 0 and self.self_lookup_behavior == SelfLookupBehavior.TRACE_ALL \
            and self.scope != ExtractionScope.ALL_RECORDS:
            # First we query up to the parents of objects we've already obtained (i.e. the targets of their lookups)
            # Then we query down to the children of all objects obtained.
            # Then we query parents and children again.
            # We repeat until we get back no new Ids, which indicates that all references have been resolved.

            # Note that the initial parent query is handled in the dependency pass above, so we start on children.

            self.context.logger.debug('%s: recursing to trace self-lookups', self.sobjectname)

            while True:
                before_count = len(self.context.get_extracted_ids(self.sobjectname))

                # Children
                for l in self.self_lookups:
                    self.perform_lookup_pass(l)

                # Parents
                self.resolve_registered_dependencies()

                after_count = len(self.context.get_extracted_ids(self.sobjectname))

                if before_count == after_count:
                    break

    def store_result(self, result):
        # Examine the received data to determine whether we have any cross-hierarchy lookups
        
        self.context.store_result(self.sobjectname, result)

        if len(self.self_lookups) > 0 and self.self_lookup_behavior == SelfLookupBehavior.TRACE_ALL:
            # Add a dependency for the reference in each self lookup of this record.
            for l in self.self_lookups:
                if result[l] is not None:
                    self.context.add_dependency(self.sobjectname, SalesforceId(result[l]))

        # Register any dependencies from dependent lookups
        if len(self.dependent_lookups) > 0:
            field_map = self.context.get_field_map(self.sobjectname)
            for f in self.dependent_lookups:
                lookup_value = result[f]
                if lookup_value is not None:
                    self.context.add_dependency(
                        field_map[f]['referenceTo'][0],
                        SalesforceId(lookup_value)
                    ) #FIXME: issue with polymorphic lookups here


    def resolve_registered_dependencies(self):
        pre_deps = self.context.get_dependencies(self.sobjectname).copy()
        self.perform_id_field_pass('Id', pre_deps)
        missing = self.context.get_dependencies(self.sobjectname).intersection(pre_deps)
        if len(missing) > 0:
            raise Exception('Unable to resolve dependencies for sObject {}. The following Ids could not be found: {}',
                self.sobjectname, ', '.join([str(i) for i in missing]))


    def perform_bulk_api_pass(self, query):
        bulk_proxy = self.context.get_bulk_proxy_object(self.sobjectname)

        results = bulk_proxy.query(query)

        # FIXME: error handling.

        for rec in results:
            self.store_result(rec)

    def perform_id_field_pass(self, id_field, id_set):
        query = 'SELECT {} FROM {} WHERE {} IN ({})'

        if len(id_set) == 0:
            return

        ids = id_set.copy()
        max_len = 4000 - len('WHERE {} IN ()'.format(self.get_field_list()))

        while len(ids) > 0:
            id_list = '\'' + str(ids.pop()) + '\''

            # The maximum length of the WHERE clause is 4,000 characters
            # Account for the length of the WHERE clause skeleton (above)
            # and iterate until we can't add another Id.
            while len(id_list) < max_len - 22 and len(ids) > 0:
                id_list += ', \'' + str(ids.pop()) + '\''

            results = self.context.connection.query_all(
                query.format(self.get_field_list(), self.sobjectname, id_field, id_list)
            )

            for rec in results.get('records'):
                self.store_result(rec)

    def perform_lookup_pass(self, field):
        self.perform_id_field_pass(
            field,
            self.context.get_sobject_ids_for_reference(self.sobjectname, field)
        )


class ExtractMapper(object):
    def __init__(self, field_name_mapping={}, field_transforms={}):
        self.field_name_mapping = field_name_mapping
        self.field_transforms = field_transforms

    def transform_record(self, record):
        return { self.transform_key(k): self.transform_value(k, record[k]) for k in record }

    def transform_key(self, k):
        return self.field_name_mapping.get(k, k)

    def transform_value(self, k, v):
        return functools.reduce(lambda x, f: f(x), self.field_transforms.get(k,[]), v)