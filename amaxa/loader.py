import csv
import simple_salesforce
import cerberus
import logging
from . import amaxa
from . import transforms

def load_credentials(incoming):
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
            security_token = credentials.get('security-token') or '',
            organizationId = credentials.get('organization-id') or '',
            sandbox = credentials.get('sandbox')
        )

        logging.getLogger('amaxa').debug('Authenticating to Salesforce with user name and password')
    elif 'access-token' in credentials and 'instance-url' in credentials:
        connection = simple_salesforce.Salesforce(instance_url=credentials['instance-url'], 
                                                  session_id=credentials['access-token'])
        logging.getLogger('amaxa').debug('Authenticating to Salesforce with access token')
    else:
        return (None, ['A set of valid credentials was not provided.'])
    
    context = amaxa.ExtractOperation(connection)
    
    return (context, [])

def load_load_operation(incoming, context):
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

        if sobject not in global_describe or not global_describe[sobject]['retrieveable']:
            errors.append('sObject {} does not exist or is not visible.'.format(sobject))
            continue

        # Determine the field scope
        lookup_behaviors = {}
        if 'field-group' in entry:
            if entry['field-group'] == 'readable':
                lam = lambda f: f['isAccessible']
            else:
                lam = lambda f: f['isUpdateable']

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
                        field_set.add(f)
                        if f not in mapped_columns:
                            mapped_columns.add(f)
                        else:
                            errors.append('Column {} is mapped to field {}.{}, but this column is already mapped.'.format(f, sobject, f))
                    else:
                        field_set.add(f['field'])
                        if 'column' in f:
                            # Note we reverse the mapper's dict for loads
                            mapper.field_name_mapping[f['column']] = f['field']
                            if f['column'] not in mapped_columns:
                                mapped_columns.add(f['column'])
                            else:
                                errors.append('Column {} is mapped to field {}.{}, but this column is already mapped.'.format(f['column'], sobject, f['field']))
                        if 'transforms' in f:
                            mapper.field_transforms[f['column']] = [getattr(transforms,t) for t in f['transforms']]
                        if 'self-lookup-behavior' in f:
                            lookup_behaviors[f['field']] = f['self-lookup-behavior']
                        if 'outside-lookup-behavior' in f:
                            lookup_behaviors[f['field']] = f['outside-lookup-behavior']
                
                context.mappers[sobject] = mapper
            else:
                field_set = set(fields)

        field_set.add('Id')

        # Validate that all fields are real and writable by this user.
        field_map = context.get_field_map(sobject)
        for f in field_set:
            if f not in field_map or not field_map[f]['isUpdateable']:
                errors.append('Field {}.{} does not exist or is not writable.'.format(sobject, f))
            elif field_map[f]['type'] == 'reference':
                # Ensure that the target objects of this reference
                # are included in the extraction. If not, show a warning.
                if any([ref not in all_sobjects for ref in field_map[f]['referenceTo']]):
                    logging.getLogger('amaxa').warn(
                        'Field %s.%s is a reference whose targets (%s) are not all included in the load. Reference handlers will be inactive for references to non-included sObjects.',
                        sobject,
                        f,
                        ', '.join(field_map[f]['referenceTo'])
                    )

        # If we've located any errors, continue to validate the rest of the extraction,
        # but don't actually create any steps or files.
        if len(errors) > 0:
            continue

        step = amaxa.LoadStep(
            sobject, 
            field_set, 
            entry['outside-lookup-behavior']
        )

        # Populate expected lookup behaviors
        for l in lookup_behaviors:
            step.set_lookup_behavior_for_field(l, lookup_behaviors[l])
            # FIXME: validate that the lookup options are applicable to this field
        
        context.add_step(step)

    if len(errors) > 0:
        return (None, errors)
    
    # Open all of the output files
    # Create DictReaders and populate them in the context
    for (s, e) in zip(context.steps, incoming['operation']):
        try:
            input = csv.DictReader(open(e['file'], 'w'), field_names=s.field_scope, extrasaction='ignore')
            context.set_input_file(s.sobjectname, input)
        except Exception as exp:
            return (None, ['Unable to open file {} for reading ({}).'.format(e['file'], exp)])

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
        lookup_behaviors = {}
        if 'field-group' in entry:
            if entry['field-group'] == 'readable':
                lam = lambda f: f['isAccessible']
            else:
                lam = lambda f: f['isUpdateable']

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
                        field_set.add(f)
                        if f not in mapped_columns:
                            mapped_columns.add(f)
                        else:
                            errors.append('Field {}.{} is mapped to column {}, but this column is already mapped.'.format(sobject, f, f))
                    else:
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
                            lookup_behaviors[f['field']] = f['self-lookup-behavior']
                        if 'outside-lookup-behavior' in f:
                            lookup_behaviors[f['field']] = f['outside-lookup-behavior']
                
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
                if any([ref not in all_sobjects for ref in field_map[f]['referenceTo']]):
                    logging.getLogger('amaxa').warn(
                        'Field %s.%s is a reference whose targets (%s) are not all included in the extraction. Reference handlers will be inactive for references to non-included sObjects.',
                        sobject,
                        f,
                        ', '.join(field_map[f]['referenceTo'])
                    )

        # If we've located any errors, continue to validate the rest of the extraction,
        # but don't actually create any steps or files.
        if len(errors) > 0:
            continue

        step = amaxa.ExtractionStep(
            sobject, 
            scope, 
            field_set, 
            query,
            entry['self-lookup-behavior'],
            entry['outside-lookup-behavior']
        )

        # Populate expected lookup behaviors
        for l in lookup_behaviors:
            step.set_lookup_behavior_for_field(l, lookup_behaviors[l])
            # FIXME: validate that the lookup options are applicable to this field
        
        context.add_step(step)

    if len(errors) > 0:
        return (None, errors)
    
    # Open all of the output files
    # Create DictWriters and populate them in the context
    for (s, e) in zip(context.steps, incoming['operation']):
        try:
            output = csv.DictWriter(open(e['file'], 'w'), fieldnames = s.field_scope, extrasaction='ignore')
            output.writeheader()
            context.set_output_file(s.sobjectname, output)
        except Exception as exp:
            return (None, ['Unable to open file {} for writing ({}).'.format(e['file'], exp)])

    return (context, [])

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
                'sdl': {
                    'type': 'string',
                    'required': False,
                    'excludes': ['fields', 'field-group']
                },
                'outside-lookup-behavior': {
                    'type': 'string',
                    'allowed': ['drop-field', 'error', 'include', 'recurse'],
                    'default': 'include'
                },
                'self-lookup-behavior': {
                    'type': 'string',
                    'allowed': ['trace-all', 'trace-none'],
                    'default': 'trace-all'
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
                                'allowed': ['drop-field', 'error', 'include', 'recurse']
                            },
                            'self-lookup-behavior': {
                                'type': 'string',
                                'allowed': ['trace-all', 'trace-none']
                            }
                        }
                    }
                }
            }
        }
    }
}
