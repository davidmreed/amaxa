import csv
import simple_salesforce
import cerberus
from . import amaxa
from . import transforms

def validate_extraction(ex, context):
    # Iterate through the extraction steps
    # Validate that all sObjects and field API names are real and visible.
    errors = []
    global_describe = context.connection.describe()["sobjects"]

    for step in ex.steps:
        if step.sobjectname not in global_describe or not global_describe[step.sobjectname]['retrieveable']:
            errors.append('sObject {} does not exist or is not visible.'.format(step.sobjectname))
        else:
            for f in step.field_scope:
                if f not in context.get_field_map(step.sobjectname):
                    errors.append('Field {}.{} does not exist or is not visible.'.format(step.sobjectname, f))
    
    return (len(errors) == 0, errors)

def load_extraction(incoming, credentials = None):
    # Inbound is raw, deserialized structures from JSON or YAML input files.
    # First, validate them against our schema and normalize them.

    incoming = normalize_extraction_schema(incoming)

    if credentials is not None:
        credentials = validate_credential_schema(credentials)
    else:
        credentials = incoming['credentials']
    
    #FIXME: handle schema errors here.

    connection = None

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
        target_file = entry.get('target-file')

        # Determine the type of extraction
        query = None
        to_extract = entry.get('extract')

        if 'ids' in to_extract:
            # Register the required IDs in the context
            for id in to_extract.get('ids'):
                context.add_dependency(sobject, amaxa.SalesforceId(id))
            
            scope = amaxa.ExtractionScope.SELECTED_RECORDS
        elif 'query' in to_extract:
            query = to_extract['query']
            scope = amaxa.ExtractionScope.QUERY
        elif 'all' in to_extract:
            scope = amaxa.ExtractionScope.ALL_RECORDS
        else:
            scope = amaxa.ExtractionScope.DESCENDENTS
        
        # Determine the field scope
        if 'field-group' in entry:
            if entry['field-group'] == 'readable':
                lam = lambda f: f['isAccessible']
            else:
                lam = lambda f: f['isUpdateable']

            field_set = set(context.get_filtered_field_map(sobject, lam).keys())
        else:
            fields = entry.get('fields')
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
                            mapper.field_transforms[f['field']] = [getattr(transforms,t) for t in f['transforms']]
                
                context.mappers[sobject] = mapper
            else:
                field_set = set(fields)

        field_set.add('Id')

        step = amaxa.SingleObjectExtraction(
            sobject, 
            scope, 
            field_set, 
            context, 
            where_clause = query
        )
        
        steps.append(step)

        # Create the output DictWriter
        output = csv.DictWriter(open(target_file, 'w'), fieldnames = fields)
        output.writeheader()
        context.set_output_file(sobject, output)

    return amaxa.MultiObjectExtraction(steps)

def validate_extraction_schema(input):
    return cerberus.Validator().validate(
        input,
        schema
    )

def normalize_extraction_schema(input):
    return cerberus.Validator.normalized(
        input,
        schema
    )

def validate_credential_schema(input):
    return cerberus.Validator().validate(
        input,
        credential_schema
    )

credential_schema = {
    'username': {
        'dependencies': ['password'],
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
        'excludes': ['username', 'password', 'security-token']
    },
    'password': {
        'dependencies': ['username'],
        'type': 'string',
        'excludes': ['access-token', 'instance-url']
    },
    'security-token': {
        'dependencies': ['username', 'password'],
        'type': 'string',
        'excludes': ['access-token', 'instance-url']
    },
    'instance-url': {
        'dependencies': ['access-token'],
        'type': 'string',
        'excludes': ['username', 'password', 'security-token']
    }
}

schema = {
    'version': {
        'type': 'integer',
        'required': True,
        'allowed': [1]
    },
    'credentials': {
        'type': 'dict',
        'required': False,
        'schema': credential_schema
    },
    'extraction': {
        'type': 'list',
        'schema': {
            'type': 'dict',
            'schema': {
                'sobject': {
                    'type': 'string',
                    'required': True
                },
                'target-file': {
                    'type': 'string',
                    'default_setter': lambda doc: doc['sobject'] + 'csv'
                },
                'sdl': {
                    'type': 'string',
                    'required': False,
                    'excludes': ['fields', 'field-group']
                },
                'outside-leaf-behavior': {
                    'type': 'string',
                    'allowed': ['blank', 'include', 'recurse'],
                    'default': 'include'
                },
                'extract': {
                    'type': 'dict',
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
                    'allowed': ['readable', 'writable'],
                    'excludes': 'sdl'
                },
                'fields': {
                    'type': 'list',
                    'excludes': 'sdl',
                    'schema': {
                        'type': ['string', 'dict'],
                        'schema': {
                            'field': {
                                'type': 'string',
                                'required': True
                            },
                            'target-column': {
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
                            'outside-leaf-behavior': {
                                'type': 'string',
                                'allowed': ['blank', 'include', 'recurse'],
                                'default': 'include'
                            }
                        }
                    }
                }
            }
        }
    }
}
