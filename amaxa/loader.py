import yaml
import csv
import transforms
import simple_salesforce
from . import amaxa

def load_extraction_from_yaml(f, credentials = None):
    incoming = yaml.sload(f)

    connection = None

    if credentials is None:
        # Obtain credentials, either from this file or from a reference in the `credentials` section
        credentials = incoming['credentials']

        if 'file' in credentials:
            # Load a different credential file
            credentials = yaml.load(credentials['file'])

    # Determine what type of credentials we have
    if 'username' in credentials and 'password' in credentials:
        # User + password, optional Security Token
        connection = simple_salesforce.Salesforce(
            username = credentials['username'],
            password = credentials['password'],
            security_token = credentials.get('security-token') or '',
            organizationId = credentials.get('organization-id'),
            sandbox = credentials.get('sandbox') or False
        )
    elif 'access-token' in credentials and 'instance-url' in credentials:
        connection = simple_salesforce.Salesforce(instance_url=credentials['instance-url'], 
                                                  session_id=credentials['access-token'])
    else:
        raise Exception('A set of valid credentials was not provided.')

    context = amaxa.OperationContext(connection)

    steps = []

    for entry in incoming['extraction']:
        sobject = entry['sobject']
        target_file = entry.get('target-file') or '{}.csv'.format(sobject)

        # Determine the type of extraction
        scope = None
        to_extract = entry.get('extract')
        query = None
        if isinstance(to_extract, str) and to_extract == 'all':
            scope = amaxa.ALL_RECORDS
        elif isinstance(to_extract, dict):
            ids = to_extract.get('ids')
            query = to_extract.get('query')
            scope = amaxa.QUERY
            if 'ids' in to_extract:
                # Register the required IDs in the context
                for id in to_extract.get(ids):
                    context.add_dependency(sobject, amaxa.SalesforceId(id))
                
                scope = amaxa.SELECTED_RECORDS
            
            if 'query' in to_extract:
                query = to_extract['query']
                scope = amaxa.QUERY
        else:
            scope = amaxa.DESCENDENTS
        
        # Determine the field scope
        fields = entry.get('fields')
        if isinstance(fields, str) and fields in ['readable', 'writeable']:
            if fields == 'readable':
                lam = lambda f: f['isAccessible']
            else:
                lam = lambda f: f['isUpdateable']

            field_set = set(context.get_filtered_field_map(sobject, lam).keys())

        # Determine whether we are doing any mapping
        if any([isinstance(f, dict) for f in fields]):
            mapper = amaxa.ExtractMapper()
            field_set = set()
            for f in fields:
                if isinstance(f, str):
                    field_set.add(f)
                else:
                    field_set.add(f['field'])
                    if 'target-column' in f:
                        mapper.field_name_mapping[f['field']] = f['target-column']
                    if 'transforms' in f:
                        mapper.field_transforms[f['field']] = [ getattr(transforms, t) for t in f['transforms'] ]
            
            context.mappers[sobject] = mapper

        field_set.add('Id')
        
        step = amaxa.ExtractionStep(entry['sobject'], 
                                    scope, 
                                    field_set, 
                                    context, 
                                    where_clause = query)
        
        steps.append(step)

        # Create the output DictWriter
        output = csv.DictWriter(open(target_file, 'w'), fieldnames = fields)
        output.writeheader()
        context.set_output_file(sobject, output)

    return amaxa.MultiObjectExtraction(steps, context)
