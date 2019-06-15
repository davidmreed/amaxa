from ... import amaxa, transforms
from .input_type import InputType


def get_available_versions(input_type):
    return SCHEMAS[input_type].keys()

def get_schema(input_type, version):
    return SCHEMAS[input_type][version]

SCHEMAS = {
    InputType.CREDENTIALS: {
        1: {
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
                        'excludes': ['username', 'password', 'security-token', 'jwt-key', 'jwt-file', 'consumer-key']
                    },
                    'password': {
                        'dependencies': ['username'],
                        'type': 'string',
                        'excludes': ['access-token', 'instance-url', 'jwt-key', 'jwt-file', 'consumer-key']
                    },
                    'security-token': {
                        'dependencies': ['username', 'password'],
                        'type': 'string',
                        'excludes': ['access-token', 'instance-url', 'jwt-key', 'jwt-file', 'consumer-key']
                    },
                    'organization-id': {
                        'dependencies': ['username', 'password'],
                        'type': 'string',
                        'excludes': ['access-token', 'instance-url', 'jwt-key', 'jwt-file', 'consumer-key']
                    },
                    'instance-url': {
                        'dependencies': ['access-token'],
                        'type': 'string',
                        'excludes': ['username', 'password', 'security-token', 'jwt-key', 'jwt-file', 'consumer-key']
                    },
                    'jwt-key': {
                        'dependencies': ['consumer-key', 'username'],
                        'type': 'string',
                        'excludes': ['password', 'security-token', 'access-token', 'instance-url', 'jwt-file']
                    },
                    'jwt-file': {
                        'dependencies': ['consumer-key', 'username'],
                        'type': 'string',
                        'excludes': ['password', 'security-token', 'access-token', 'instance-url', 'jwt-key']
                    },
                    'consumer-key': {
                        'dependencies': ['username'],
                        'type': 'string',
                        'excludes': ['password', 'security-token', 'access-token', 'instance-url']
                    }
                }
            }
        }
    },
    InputType.LOAD_OPERATION: {
        1: {
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
                        'result-file': {
                            'type': 'string',
                            'default_setter': lambda doc: doc['sobject'] + '-results.csv'
                        },
                        'input-validation': {
                            'type': 'string',
                            'default': 'default',
                            'allowed': ['none', 'default', 'strict']
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
                            'allowed': ['readable', 'writeable', 'smart'],
                            'excludes': ['fields']
                        },
                        'fields': {
                            'type': 'list',
                            'excludes': ['field-group'],
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
    },
    InputType.STATE: {
        1: {
            'version': {
                'type': 'integer',
                'required': True,
                'allowed': [1]
            },
            'state': {
                'type': 'dict',
                'required': True,
                'schema': {
                    'stage': {
                        'type': 'string',
                        'required': True,
                        'allowed': amaxa.LoadStage.all_values()
                    },
                    'id-map': {
                        'type': 'dict',
                        'required': True
                    }
                }
            }
        }
    }
}

SCHEMAS[InputType.EXTRACT_OPERATION]  = SCHEMAS[InputType.LOAD_OPERATION]
