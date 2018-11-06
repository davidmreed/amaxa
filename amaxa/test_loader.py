import unittest
import simple_salesforce
from unittest.mock import Mock
from . import amaxa, loader

class test_load_credentials(unittest.TestCase):
    def test_credential_schema_validates_username_password(self):
        (result, errors) = loader.validate_credential_schema(
            {
                'version': 1,
                'credentials': {
                    'password': '123456',
                    'username': 'baltar@ucaprica.cc',
                    'security-token': '98765',
                    'sandbox': True
                }
            }
        )

        self.assertIsNotNone(result)
        self.assertEqual([], errors)

    def test_credential_schema_validates_access_token(self):
        (result, errors) = loader.validate_credential_schema(
            {
                'version': 1,
                'credentials': {
                    'access-token': 'ABCDEF123456',
                    'instance-url': 'test.salesforce.com'
                }
            }
        )

        self.assertIsNotNone(result)
        self.assertEqual([], errors)

    def test_credential_schema_fails_mixed_values(self):
        (result, errors) = loader.validate_credential_schema(
            {
                'version': 1, 
                'credentials': {
                    'password': '123456',
                    'username': 'baltar@ucaprica.cc',
                    'security-token': '98765',
                    'sandbox': True,
                    'instance-url': 'test.salesforce.com'
                }
            }
        )

        self.assertIsNone(result)
        self.assertGreater(len(errors), 0)

    def test_validate_credential_schema_returns_normalized_input(self):
        credentials = {
            'version': 1,
            'credentials': {
                'username': 'baltar@ucaprica.edu',
                'password': '666666666'
            }
        }

        (result, errors) = loader.validate_credential_schema(credentials)

        self.assertEqual(False, result['credentials']['sandbox'])
        self.assertEqual([], errors)

    def test_validate_credential_schema_returns_errors(self):
        credentials = {
            'credentials': {
                'username': 'baltar@ucaprica.edu',
                'password': '666666666'
            }
        }

        (result, errors) = loader.validate_credential_schema(credentials)

        self.assertIsNone(result)
        self.assertEqual(['version: [\'required field\']'], errors)

    @unittest.mock.patch('simple_salesforce.Salesforce')
    def test_load_credentials_uses_username_password(self, sf_mock):
        (result, errors) = loader.load_credentials(
            {
                'version': 1,
                'credentials': {
                    'password': '123456',
                    'username': 'baltar@ucaprica.cc',
                    'security-token': '98765',
                    'sandbox': True
                }
            }
        )

        self.assertEqual([], errors)
        self.assertIsNotNone(result)
        
        sf_mock.assert_called_once_with(
            username='baltar@ucaprica.cc',
            password='123456',
            security_token='98765',
            organizationId='',
            sandbox=True
        )

    @unittest.mock.patch('simple_salesforce.Salesforce')
    def test_load_credentials_uses_access_token(self, sf_mock):
        (result, errors) = loader.load_credentials(
            {
                'version': 1,
                'credentials': {
                    'access-token': 'ABCDEF123456',
                    'instance-url': 'test.salesforce.com'
                }
            }
        )

        self.assertEqual([], errors)
        self.assertIsNotNone(result)
        
        sf_mock.assert_called_once_with(
            session_id='ABCDEF123456',
            instance_url='test.salesforce.com'
        )

    @unittest.mock.patch('simple_salesforce.Salesforce')
    def test_load_credentials_returns_validation_errors(self, sf_mock):
        credentials = {
            'credentials': {
                'username': 'baltar@ucaprica.edu',
                'password': '666666666'
            }
        }

        (result, errors) = loader.load_credentials(credentials)

        self.assertIsNone(result)
        self.assertEqual(['version: [\'required field\']'], errors)

    @unittest.mock.patch('simple_salesforce.Salesforce')
    def test_load_credentials_returns_error_without_credentials(self, sf_mock):
        credentials = {
            'version': 1,
            'credentials': {
            }
        }

        (result, errors) = loader.load_credentials(credentials)

        self.assertIsNone(result)
        self.assertEqual(['A set of valid credentials was not provided.'], errors)


class test_load_extraction(unittest.TestCase):
    def test_validate_extraction_schema_returns_normalized_input(self):
        (result, errors) = loader.validate_extraction_schema(
            {
                'version': 1,
                'extraction': [
                    { 
                        'sobject': 'Account',
                        'fields': [ 'Name', 'ParentId' ],
                        'extract': { 'all': True }
                    }
                ]
            }
        )

        self.assertEqual('Account.csv', result['extraction'][0]['target-file'])
        self.assertEqual([], errors)

    def test_validate_extraction_schema_returns_errors(self):
        (result, errors) = loader.validate_extraction_schema(
            {
                'extraction': [
                    { 
                        'sobject': 'Account',
                        'fields': [ 'Name', 'ParentId' ],
                        'extract': { 'all': True }
                    }
                ]
            }
        )

        self.assertIsNone(result)
        self.assertEqual(['version: [\'required field\']'], errors)

    def test_load_extraction_returns_validation_errors(self):
        context = Mock()
        (result, errors) = loader.load_extraction(
            {
                'extraction': [
                    { 
                        'sobject': 'Account',
                        'fields': [ 'Name', 'ParentId' ],
                        'extract': { 'all': True }
                    }
                ]
            },
            context
        )

        self.assertIsNone(result)
        self.assertEqual(['version: [\'required field\']'], errors)
        context.assert_not_called()

    @unittest.mock.patch('simple_salesforce.Salesforce')
    def test_load_extraction_traps_login_exceptions(self, sf_mock):
        return_exception = simple_salesforce.SalesforceAuthenticationFailed(500, 'Internal Server Error')
        sf_mock.describe = Mock(side_effect=return_exception)
        context = amaxa.OperationContext(sf_mock)
        ex = {
            'version': 1,
            'extraction': [
                { 
                    'sobject': 'Account',
                    'field-group': 'readable',
                    'extract': { 'all': True }
                }
            ]
        }

        (result, errors) = loader.load_extraction(ex, context)

        self.assertIsNone(result)
        self.assertEqual(['Unable to authenticate to Salesforce: {}'.format(return_exception)], errors)

    def test_load_extraction_flags_missing_sobjects(self):
        context = Mock()
        context.connection.describe = Mock(
            return_value={
                'sobjects': {
                    'Account': {
                        'retrieveable': True
                    }
                }
            }
        )
        context.get_field_map = Mock(
            return_value={ 
                'Name': {
                    'type': 'string'
                },
                'Id': {
                    'type': 'string'
                }
            }
        )

        ex = { 
            'version': 1, 
            'extraction': [
                { 
                    'sobject': 'Account',
                    'fields': [ 'Name' ],
                    'extract': { 'all': True }
                },
                {
                    'sobject': 'Contact',
                    'fields': [ 'Name' ],
                    'extract': { 'all': True }
                },
                {
                    'sobject': 'Opportunity',
                    'fields': [ 'Name' ],
                    'extract': { 'all': True }
                }
            ]
        }
        m = unittest.mock.mock_open()
        with unittest.mock.patch('builtins.open', m):
            (result, errors) = loader.load_extraction(ex, context)
        
        m.assert_not_called()

        self.assertIsNone(result)
        self.assertEqual(
            [
                'sObject Contact does not exist or is not visible.',
                'sObject Opportunity does not exist or is not visible.'
            ],
            errors
        )
    
    def test_validate_extraction_flags_missing_fields(self):
        context = Mock()
        context.connection.describe = Mock(
            return_value={
                'sobjects': {
                    'Account': {
                        'retrieveable': True
                    }
                }
            }
        )
        context.get_field_map = Mock(
            return_value={ 
                'Name': {
                    'type': 'string'
                },
                'Id': {
                    'type': 'string'
                }
            }
        )

        ex = { 
            'version': 1, 
            'extraction': [
                { 
                    'sobject': 'Account',
                    'fields': [ 'Name', 'ParentId' ],
                    'extract': { 'all': True }
                }
            ]
        }

        m = unittest.mock.mock_open()
        with unittest.mock.patch('builtins.open', m):
            (result, errors) = loader.load_extraction(ex, context)

        m.assert_not_called()

        self.assertIsNone(result)
        self.assertEqual(
            [
                'Field Account.ParentId does not exist or is not visible.'
            ],
            errors
        )

    def test_load_extraction_creates_valid_steps(self):
        # FIXME: add a Query and a Descendents step to this unit test.
        context = amaxa.OperationContext(Mock())
        context.connection.describe = Mock(
            return_value={
                'sobjects': {
                    'Account': {
                        'retrieveable': True
                    },
                    'Contact': {
                        'retrieveable': True
                    }
                }
            }
        )
        context.get_field_map = Mock(
            return_value={ 
                'Name': {
                    'type': 'string'
                },
                'Id': {
                    'type': 'string'
                }
            }
        )
        context.add_dependency = Mock()

        ex = {
            'version': 1,
            'extraction': [
                { 
                    'sobject': 'Account',
                    'fields': [ 'Name' ],
                    'extract': { 'all': True }
                },
                { 
                    'sobject': 'Contact',
                    'fields': [ 'Name' ],
                    'extract': { 
                        'ids': [
                            '003000000000000',
                            '003000000000001'
                        ]
                    }
                }

            ]
        }

        m = unittest.mock.mock_open()
        with unittest.mock.patch('builtins.open', m):
            (result, errors) = loader.load_extraction(ex, context)
            
        m.assert_has_calls(
            [
                unittest.mock.call('Account.csv', 'w'),
                unittest.mock.call('Contact.csv', 'w')
            ],
            any_order=True
        )

        context.add_dependency.assert_has_calls(
            [
                unittest.mock.call('Contact', amaxa.SalesforceId('003000000000000')),
                unittest.mock.call('Contact', amaxa.SalesforceId('003000000000001'))
            ]
        )

        self.assertIsInstance(result, amaxa.OperationContext)
        self.assertEqual([], errors)
        self.assertEqual(2, len(result.steps))
        self.assertEqual('Account', result.steps[0].sobjectname)
        self.assertEqual(amaxa.ExtractionScope.ALL_RECORDS, result.steps[0].scope)
        self.assertEqual('Contact', result.steps[1].sobjectname)
        self.assertEqual(amaxa.ExtractionScope.SELECTED_RECORDS, result.steps[1].scope)

    def test_load_extraction_finds_readable_field_group(self):
        context = amaxa.OperationContext(Mock())
        context.connection.describe = Mock(
            return_value={
                'sobjects': {
                    'Account': {
                        'retrieveable': True
                    }
                }
            }
        )
        context.get_field_map = Mock(
            return_value={ 
                'Name': {
                    'type': 'string',
                    'isAccessible': True
                },
                'Id': {
                    'type': 'string',
                    'isAccessible': True
                },
                'Industry': {
                    'type': 'string',
                    'isAccessible': False
                }
            }
        )

        ex = {
            'version': 1,
            'extraction': [
                { 
                    'sobject': 'Account',
                    'field-group': 'readable',
                    'extract': { 'all': True }
                }
            ]
        }

        m = unittest.mock.mock_open()
        with unittest.mock.patch('builtins.open', m):
            (result, errors) = loader.load_extraction(ex, context)
            
        m.assert_called_once_with('Account.csv', 'w')

        self.assertIsInstance(result, amaxa.OperationContext)
        self.assertEqual([], errors)

        self.assertEqual({'Name', 'Id'}, result.steps[0].field_scope)

    def test_load_extraction_finds_writeable_field_group(self):
        context = amaxa.OperationContext(Mock())
        context.connection.describe = Mock(
            return_value={
                'sobjects': {
                    'Account': {
                        'retrieveable': True
                    }
                }
            }
        )
        context.get_field_map = Mock(
            return_value={ 
                'Name': {
                    'type': 'string',
                    'isUpdateable': True
                },
                'Id': {
                    'type': 'string',
                    'isUpdateable': True
                },
                'Industry': {
                    'type': 'string',
                    'isUpdateable': False
                }
            }
        )

        ex = {
            'version': 1,
            'extraction': [
                { 
                    'sobject': 'Account',
                    'field-group': 'writable',
                    'extract': { 'all': True }
                }
            ]
        }

        m = unittest.mock.mock_open()
        with unittest.mock.patch('builtins.open', m):
            (result, errors) = loader.load_extraction(ex, context)
            
        m.assert_called_once_with('Account.csv', 'w')

        self.assertIsInstance(result, amaxa.OperationContext)
        self.assertEqual([], errors)

    def test_load_extraction_generates_field_list(self):
        connection = Mock()
        context = amaxa.OperationContext(connection)
        context.connection.describe = Mock(
            return_value={
                'sobjects': {
                    'Account': {
                        'retrieveable': True
                    }
                }
            }
        )
        context.get_field_map = Mock(
            return_value={ 
                'Name': {
                    'type': 'string'
                },
                'Id': {
                    'type': 'string'
                },
                'Industry': {
                    'type': 'string'
                }
            }
        )

        ex = {
            'version': 1,
            'extraction': [
                { 
                    'sobject': 'Account',
                    'fields': [
                        'Name', 
                        'Industry'
                    ],
                    'extract': { 'all': True }
                }
            ]
        }

        m = unittest.mock.mock_open()
        with unittest.mock.patch('builtins.open', m):
            (result, errors) = loader.load_extraction(ex, context)
            
        m.assert_called_once_with('Account.csv', 'w')

        self.assertIsInstance(result, amaxa.OperationContext)
        self.assertEqual([], errors)

        self.assertEqual({'Name', 'Industry', 'Id'}, result.steps[0].field_scope)

    def test_load_extraction_creates_export_mapper(self):
        context = amaxa.OperationContext(Mock())
        context.connection.describe = Mock(
            return_value={
                'sobjects': {
                    'Account': {
                        'retrieveable': True
                    }
                }
            }
        )
        context.get_field_map = Mock(
            return_value={ 
                'Name': {
                    'type': 'string'
                },
                'Id': {
                    'type': 'string'
                },
                'Industry': {
                    'type': 'string'
                }
            }
        )

        ex = {
            'version': 1,
            'extraction': [
                { 
                    'sobject': 'Account',
                    'fields': [
                        {
                            'field': 'Name',
                            'column': 'Account Name',
                            'transforms': ['strip', 'lowercase']
                        },
                        'Industry'
                    ],
                    'extract': { 'all': True }
                }
            ]
        }

        m = unittest.mock.mock_open()
        with unittest.mock.patch('builtins.open', m):
            (result, errors) = loader.load_extraction(ex, context)
            
        m.assert_called_once_with('Account.csv', 'w')

        self.assertIsInstance(result, amaxa.OperationContext)
        self.assertEqual([], errors)

        self.assertEqual({'Name', 'Industry', 'Id'}, result.steps[0].field_scope)
        self.assertIn('Account', context.mappers)

        mapper = context.mappers['Account']
        self.assertEqual(
            {'Account Name': 'university of caprica', 'Industry': 'Education'},
            mapper.transform_record({ 'Name': 'UNIversity of caprica  ', 'Industry': 'Education' })
        )

    def test_load_extraction_catches_duplicate_columns(self):
        context = amaxa.OperationContext(Mock())
        context.connection.describe = Mock(
            return_value={
                'sobjects': {
                    'Account': {
                        'retrieveable': True
                    }
                }
            }
        )
        context.get_field_map = Mock(
            return_value={ 
                'Name': {
                    'type': 'string'
                },
                'Id': {
                    'type': 'string'
                },
                'Industry': {
                    'type': 'string'
                },
                'AccountSite': {
                    'type': 'string'
                }
            }
        )

        ex = {
            'version': 1,
            'extraction': [
                { 
                    'sobject': 'Account',
                    'fields': [
                        {
                            'field': 'Name',
                            'column': 'Industry',
                        },
                        'Industry',
                        {
                            'field': 'AccountSite',
                            'column': 'Industry'
                        }
                    ],
                    'extract': { 'all': True }
                }
            ]
        }

        m = unittest.mock.mock_open()
        with unittest.mock.patch('builtins.open', m):
            (result, errors) = loader.load_extraction(ex, context)

        self.assertIsNone(result)
        self.assertEqual(
            ['Field Account.Industry is mapped to column Industry, but this column is already mapped.',
            'Field Account.AccountSite is mapped to column Industry, but this column is already mapped.'],
            errors
        )

    def test_load_extraction_populates_lookup_behaviors(self):
        context = amaxa.OperationContext(Mock())
        context.connection.describe = Mock(
            return_value={
                'sobjects': {
                    'Account': {
                        'retrieveable': True
                    },
                    'Contact': {
                        'retrieveable': True
                    }
                }
            }
        )
        context.get_field_map = Mock(
            return_value={ 
                'Name': {
                    'type': 'string'
                },
                'Id': {
                    'type': 'string'
                },
                'ParentId': {
                    'type': 'reference',
                    'referenceTo': ['Account']
                },
                'Primary_Contact__c': {
                    'type': 'reference',
                    'referenceTo': ['Contact']
                }
            }
        )

        ex = {
            'version': 1,
            'extraction': [
                { 
                    'sobject': 'Account',
                    'fields': [
                        'Name',
                        {
                            'field': 'ParentId',
                            'self-lookup-behavior': 'trace-none'
                        },
                        {
                            'field': 'Primary_Contact__c',
                            'outside-lookup-behavior': 'drop-field'
                        }
                    ],
                    'extract': { 'all': True }
                },
                {
                    'sobject': 'Contact',
                    'fields': [
                        {
                            'field': 'Name'
                        }
                    ],
                    'extract': { 'all': True }
                }
            ]
        }

        m = unittest.mock.mock_open()
        with unittest.mock.patch('builtins.open', m):
            (result, errors) = loader.load_extraction(ex, context)

        self.assertIsInstance(result, amaxa.OperationContext)
        self.assertEqual([], errors)

        self.assertEqual(amaxa.SelfLookupBehavior.TRACE_NONE, result.steps[0].get_self_lookup_behavior_for_field('ParentId'))
        self.assertEqual(amaxa.OutsideLookupBehavior.DROP_FIELD, result.steps[0].get_outside_lookup_behavior_for_field('Primary_Contact__c'))

    def test_load_extraction_validates_lookup_behaviors(self):
        pass #FIXME: Implement. Currently, invalid values are just treated like the default.

    @unittest.mock.patch('logging.getLogger')
    def test_load_extraction_warns_lookups_other_objects(self, logger):
        context = amaxa.OperationContext(Mock())
        amaxa_logger = Mock()
        logger.return_value=amaxa_logger
        context.connection.describe = Mock(
            return_value={
                'sobjects': {
                    'Account': {
                        'retrieveable': True
                    },
                    'Contact': {
                        'retrieveable': True
                    },
                    'Test__c': {
                        'retrieveable': True
                    }
                }
            }
        )
        context.get_field_map = Mock(
            return_value={ 
                'Id': {
                    'type': 'string'
                },
                'Parent__c': {
                    'type': 'reference',
                    'referenceTo': ['Parent__c']
                }
            }
        )

        ex = {
            'version': 1,
            'extraction': [
                { 
                    'sobject': 'Test__c',
                    'fields': [ 'Parent__c' ],
                    'extract': { 'all': True }
                }
            ]
        }

        m = unittest.mock.mock_open()
        with unittest.mock.patch('builtins.open', m):
            (result, errors) = loader.load_extraction(ex, context)

        self.assertIsInstance(result, amaxa.OperationContext)
        self.assertEqual([], errors)
        amaxa_logger.warn.assert_called_once_with(
            'Field %s.%s is a reference whose targets (%s) are not all included in the extraction. Reference handlers will be inactive for references to non-included sObjects.',
            'Test__c',
            'Parent__c',
            ', '.join(['Parent__c'])
        )

