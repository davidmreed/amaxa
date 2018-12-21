import csv
import simple_salesforce
import cerberus
import logging
from . import amaxa
from . import transforms

def load_credentials(incoming, load):
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
    
    if not load:
        context = amaxa.ExtractOperation(connection)
    else:
        context = amaxa.LoadOperation(connection)
    
    return (context, [])

def load_load_operation(incoming, context):
    # Inbound is raw, deserialized structures from JSON or YAML input files.
    # First, validate them against our schema and normalize them.

    (incoming, errors) = validate_load_schema(incoming)
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

        if sobject not in global_describe or not global_describe[sobject]['createable']:
            errors.append('sObject {} does not exist, is not visible, or is not createable.'.format(sobject))
            continue

        # Determine the field scope
        lookup_behaviors = {}
        if 'field-group' in entry:
            # Validation clamps acceptable values to 'writeable' or 'smart' by this point.
            lam = lambda f: f['createable']

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
                        if f in field_set:
                            errors.append('Field {}.{} is present more than once in the specification.'.format(sobject, f))
                        if f in mapped_columns:
                            errors.append('Column {} is mapped to field {}.{}, but this column is already mapped.'.format(f, sobject, f))

                        field_set.add(f)
                        mapped_columns.add(f)
                    else:
                        if f['field'] in field_set:
                            errors.append('Field {}.{} is present more than once in the specification.'.format(sobject, f['field']))

                        field_set.add(f['field'])

                        if 'column' in f:
                            if f['column'] in mapped_columns:
                                errors.append('Column {} is mapped to field {}.{}, but this column is already mapped.'.format(f['column'], sobject, f['field']))

                            # Note we reverse the mapper's dict for loads
                            mapper.field_name_mapping[f['column']] = f['field']
                            mapped_columns.add(f['column'])
                        if 'transforms' in f:
                            mapper.field_transforms[f['column']] = [getattr(transforms,t) for t in f['transforms']]
                        if 'self-lookup-behavior' in f:
                            lookup_behaviors[f['field']] = amaxa.SelfLookupBehavior.values_dict()[f['self-lookup-behavior']]
                        if 'outside-lookup-behavior' in f:
                            lookup_behaviors[f['field']] = amaxa.OutsideLookupBehavior.values_dict()[f['outside-lookup-behavior']]
                
                context.mappers[sobject] = mapper
            else:
                field_set = set(fields)

        # Validate that all fields are real and writeable by this user.
        field_map = context.get_field_map(sobject)
        for f in field_set:
            if f not in field_map or not field_map[f]['createable']:
                errors.append('Field {}.{} does not exist, is not writeable, or is not visible.'.format(sobject, f))
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
            amaxa.OutsideLookupBehavior.values_dict()[entry['outside-lookup-behavior']]
        )

        # Populate expected lookup behaviors
        for l in lookup_behaviors:
            step.set_lookup_behavior_for_field(l, lookup_behaviors[l])
        
        context.add_step(step)

    for step in context.steps:
        step.scan_fields()

    validate_dependent_field_permissions(context, errors)
    validate_lookup_behaviors(context.steps, errors)

    if len(errors) > 0:
        return (None, errors)
    
    # Open all of the input files
    # Create DictReaders and populate them in the context
    for (s, e) in zip(context.steps, incoming['operation']):
        try:
            input = csv.DictReader(open(e['file'], 'r'))
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
            if entry['field-group'] in ['readable', 'smart']:
                lam = lambda f: True
            else:
                lam = lambda f: f['createable']

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
                        if f in field_set:
                            errors.append('Field {}.{} is present more than once in the specification.'.format(sobject, f))

                        field_set.add(f)
                        if f not in mapped_columns:
                            mapped_columns.add(f)
                        else:
                            errors.append('Field {}.{} is mapped to column {}, but this column is already mapped.'.format(sobject, f, f))
                    else:
                        if f['field'] in field_set:
                            errors.append('Field {}.{} is present more than once in the specification.'.format(sobject, f['field']))

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
                            lookup_behaviors[f['field']] = amaxa.SelfLookupBehavior.values_dict()[f['self-lookup-behavior']]
                        if 'outside-lookup-behavior' in f:
                            lookup_behaviors[f['field']] = amaxa.OutsideLookupBehavior.values_dict()[f['outside-lookup-behavior']]
                
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
            amaxa.SelfLookupBehavior.values_dict()[entry['self-lookup-behavior']],
            amaxa.OutsideLookupBehavior.values_dict()[entry['outside-lookup-behavior']]
        )

        # Populate expected lookup behaviors
        for l in lookup_behaviors:
            step.set_lookup_behavior_for_field(l, lookup_behaviors[l])
        
        context.add_step(step)

    for step in context.steps:
        step.scan_fields()
    validate_lookup_behaviors(context.steps, errors)

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

def validate_dependent_field_permissions(context, errors):
    for step in context.steps:
        field_map = context.get_field_map(step.sobjectname)
        for f in step.dependent_lookups:
            if not field_map[f]['updateable']:
                errors.append('Field {}.{} is a dependent lookup, but is not updateable.'.format(step.sobjectname, f))


def validate_lookup_behaviors(steps, errors):
    # Scan fields for each step (populate the various lookup collections)
    # so we can validate the lookup behaviors.
    for step in steps:
        for f in step.lookup_behaviors:
            if (f in step.dependent_lookups and step.lookup_behaviors[f] not in amaxa.OutsideLookupBehavior) \
                or (f in step.self_lookups and step.lookup_behaviors[f] not in amaxa.SelfLookupBehavior):
                errors.append('Lookup behavior \'{}\' specified for field {}.{} is not valid for this lookup type.'.format(
                    step.lookup_behaviors[f].value,
                    step.sobjectname,
                    f
                ))

def validate_extraction_schema(input):
    v = cerberus.Validator(get_operation_schema(True))
    return (
        v.validated(input),
        ['{}: {}'.format(k, v.errors[k]) for k in v.errors]
    )

def validate_load_schema(input):
    v = cerberus.Validator(get_operation_schema(False))
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

def get_operation_schema(is_extract = True):
    return {
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
                        'allowed': amaxa.OutsideLookupBehavior.all_values(),
                        'default': 'include'
                    },
                    'self-lookup-behavior': {
                        'type': 'string',
                        'allowed': amaxa.SelfLookupBehavior.all_values(),
                        'default': 'trace-all'
                    },
                    'extract': {
                        'type': 'dict',
                        'required': is_extract,
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
                        'allowed': ['readable', 'writeable', 'smart'] if is_extract else ['writeable', 'smart'],
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
                                    'allowed': amaxa.OutsideLookupBehavior.all_values()
                                },
                                'self-lookup-behavior': {
                                    'type': 'string',
                                    'allowed': amaxa.SelfLookupBehavior.all_values()
                                }
                            }
                        }
                    }
                }
            }
        }
    }
