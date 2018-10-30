import csv
import simple_salesforce
import cerberus
from . import amaxa
from . import transforms

def load_credentials(incoming):
    (credentials, errors) = validate_credential_schema(incoming)
    if credentials is None:
        return (None, errors)

    connection = None

    # Determine what type of credentials we have
    if 'username' in credentials and 'password' in credentials:
        # User + password, optional Security Token
        connection = simple_salesforce.Salesforce(
            username = credentials['username'],
            password = credentials['password'],
            security_token = credentials.get('security-token') or '',
            organizationId = credentials.get('organization-id') or '',
            sandbox = credentials.get('sandbox')
        )
    elif 'access-token' in credentials and 'instance-url' in credentials:
        connection = simple_salesforce.Salesforce(instance_url=credentials['instance-url'], 
                                                  session_id=credentials['access-token'])
    else:
        return (None, ['A set of valid credentials was not provided.'])
    
    context = amaxa.OperationContext(connection)
    
    return (context, [])

def load_extraction(incoming, context):
    # Inbound is raw, deserialized structures from JSON or YAML input files.
    # First, validate them against our schema and normalize them.

    (incoming, errors) = validate_extraction_schema(incoming)
    if incoming is None:
        return (None, errors)
 
    global_describe = context.connection.describe()["sobjects"]

    steps = []
    errors = []

    for entry in incoming['extraction']:
        sobject = entry['sobject']

        if sobject not in global_describe or not global_describe[sobject]['retrieveable']:
            errors.append('sObject {} does not exist or is not visible.'.format(sobject))
            continue

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

        # Validate that all fields are real and readable by this user.
        for f in field_set:
            if f not in context.get_field_map(sobject):
                errors.append('Field {}.{} does not exist or is not visible.'.format(sobject, f))
        
        # If we've located any errors, continue to validate the rest of the extraction,
        # but don't actually create any steps or files.
        if len(errors) > 0:
            continue

        step = amaxa.SingleObjectExtraction(
            sobject, 
            scope, 
            field_set, 
            context, 
            where_clause = query
        )
        
        steps.append(step)

    if len(errors) > 0:
        return (None, errors)
    
    # Open all of the output files
    # Create DictWriters and populate them in the context
    for (s, e) in zip(steps, incoming['extraction']):
        try:
            output = csv.DictWriter(open(e['target-file'], 'w'), fieldnames = s.field_scope)
            output.writeheader()
            context.set_output_file(s.sobjectname, output)
        except Exception as e:
            return (None, ['Unable to open file {} for writing ({}).'.format(target_file, e)])

    ex = amaxa.MultiObjectExtraction(steps)
    return (ex, [])

def validate_extraction_schema(input):
    v = cerberus.Validator(schema)
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
            'organization-id': {
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
    }
}

schema = {
    'version': {
        'type': 'integer',
        'required': True,
        'allowed': [1]
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
                    'default_setter': lambda doc: doc['sobject'] + '.csv'
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
                    'required': True,
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
                    'excludes': ['sdl', 'fields']
                },
                'fields': {
                    'type': 'list',
                    'excludes': ['sdl' 'field-group'],
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
