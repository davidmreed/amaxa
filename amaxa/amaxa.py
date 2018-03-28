#!/usr/bin/env python3

import simple_salesforce
import csv
import argparse

class SalesforceId(object):
    def __init__(self, idstr):
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
        self.connection = connection
        self.describe_info = {}
        self.field_maps = {}
        self.proxy_objects = {}
        self.required_ids = {}

    def add_dependency(self, sobjectname, id):
        if sobjectname not in self.required_ids:
            self.required_ids[sobjectname] = set()
        
        self.required_ids[sobjectname].add(id)
    
    def get_dependencies(self, sobjectname)
        return self.required_ids[sobjectname] or set()

    def get_proxy_object(self, sobjectname):
        if sobjectname not in self.proxy_objects:
            self.proxy_objects[sobjectname] = self.connection.SFType(sobjectname)

        return self.proxy_objects[sobjectname]
    
    def add_result(self, sobjectname, record):
        pass
    
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
        pass

class Extraction(object):
    def __init__(self, steps, context):
        self.steps = steps
        self.context = context
        self.extracted_ids = {}

    def execute(self):
        for s in steps:
            s.execute()

class Import(object):
    pass

class ExtractionStep(object):
    def __init__(self, sobjectname, scope, field_scope, context, where_clause = None):
        self.sobjectname = sobjectname
        self.scope = scope
        self.field_scope = field_scope
        self.context = context
        self.where_clause = where_clause

    def get_field_list(self):
        return ', '.join(self.field_scope)

    def execute(self):
        # If scope if ALL_RECORDS, execute a Bulk API job to extract all records
        # If scope is QUERY, execute a Bulk API job to download a query with where_clause
        # If scope is DESCENDENTS, pull based on objects that look up to any already
        # extracted object.
        # If scope is SELECTED_RECORDS, and if `context` has any registered dependencies, 
        # perform a query to extract those records by Id.

        proxy = context.get_proxy_object(self.sobjectname)
        if self.scope == ALL_RECORDS:
            self.perform_bulk_api_pass(
                'SELECT {} FROM {}'.format(self.get_field_list(), self.sobjectname)
            )

            # FIXME: add the results
        elif self.scope == QUERY:
            self.perform_bulk_api_pass(
                'SELECT {} FROM {} WHERE {}'.format(self.get_field_list(), self.sobjectname)
            )
        elif self.scope == DESCENDENTS:
            lookups = context.get_filtered_field_map(self.sobjectname, lambda f: f['type'] == 'reference')
            for f in self.field_scope:
                if f in lookups:
                    self.perform_lookup_pass(f)


        # Fall through to grab all dependencies or SELECTED_RECORDS

        self.perform_id_field_pass('Id', self.context.get_dependencies(self.sobjectname))
    
    def perform_bulk_api_pass(self, query):
        bulk_proxy = self.context.get_bulk_proxy(self.sobjectname)

        bulk_proxy.query(query)

        # FIXME: add the results

    def perform_id_field_pass(self, id_field, id_set):
        query = 'SELECT {} FROM {} WHERE {} IN ({})'

        ids = id_set.copy()

        id_list = ''

        while len(ids) > 0:
            id_list = '\'' + ids.pop() + '\''

            # The maximum length of the WHERE clause is 4,000 characters
            while len(id_list) < 3980 and len(ids) > 0:
                id_list += ', \'' + ids.pop() + '\''

            results = self.context.connection.query_all(
                query.format(self.get_field_list(), self.sobjectname, id_field, id_list)
            )

            for rec in results.get('records'):
                self.context.add_result(self.sobjectname, rec)
    
    def perform_lookup_pass(self, field):
        self.perform_id_field_pass(self, field, 
            self.context.get_sobject_ids_for_reference(self.sobjectname, self.field))          

class ExportMapper(object):
    def __init__(self, field_name_mapping, field_transforms={}):
        self.field_name_mapping = field_name_mapping
        self.field_transforms = field_transforms

    def transform_record(self, record):
        return { field_name_mapping[k] : record[k] for k in self.field_name_mapping }


class ImportMapper(object):
    def __init__(self, field_name_mapping, field_transforms={}):
        self.field_name_mapping = field_name_mapping
        self.field_transforms = field_transforms

    def transform_record(self, record):
        final_record = {}
        final_name = self.field_name_mapping[k]

        for k in self.field_name_mapping:
            val = record[k]

            if final_name in self.field_transforms:
                for lam in self.field_transforms[final_name]:
                    val = lam(val)

            val = self.typecast_for_field(final_name)(val)

            final_record[final_name] = val 
        
        return final_record