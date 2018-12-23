import unittest
import simple_salesforce
import io
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
            },
            False
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
            },
            False
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

        (result, errors) = loader.load_credentials(credentials, False)

        self.assertIsNone(result)
        self.assertEqual(['version: [\'required field\']'], errors)

    @unittest.mock.patch('simple_salesforce.Salesforce')
    def test_load_credentials_returns_error_without_credentials(self, sf_mock):
        credentials = {
            'version': 1,
            'credentials': {
            }
        }

        (result, errors) = loader.load_credentials(credentials, False)

        self.assertIsNone(result)
        self.assertEqual(['A set of valid credentials was not provided.'], errors)


class test_load_extraction_operation(unittest.TestCase):
    def test_validate_extraction_schema_returns_normalized_input(self):
        (result, errors) = loader.validate_extraction_schema(
            {
                'version': 1,
                'operation': [
                    { 
                        'sobject': 'Account',
                        'fields': [ 'Name', 'ParentId' ],
                        'extract': { 'all': True }
                    }
                ]
            }
        )

        self.assertEqual('Account.csv', result['operation'][0]['file'])
        self.assertEqual([], errors)

    def test_validate_extraction_schema_returns_errors(self):
        (result, errors) = loader.validate_extraction_schema(
            {
                'operation': [
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

    def test_load_extraction_operation_returns_validation_errors(self):
        context = Mock()
        (result, errors) = loader.load_extraction_operation(
            {
                'operation': [
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
    def test_load_extraction_operation_traps_login_exceptions(self, sf_mock):
        return_exception = simple_salesforce.SalesforceAuthenticationFailed(500, 'Internal Server Error')
        sf_mock.describe = Mock(side_effect=return_exception)
        context = amaxa.ExtractOperation(sf_mock)
        ex = {
            'version': 1,
            'operation': [
                { 
                    'sobject': 'Account',
                    'field-group': 'readable',
                    'extract': { 'all': True }
                }
            ]
        }

        (result, errors) = loader.load_extraction_operation(ex, context)

        self.assertIsNone(result)
        self.assertEqual(['Unable to authenticate to Salesforce: {}'.format(return_exception)], errors)

    def test_load_extraction_operation_flags_missing_sobjects(self):
        context = Mock()
        context.steps = []
        context.connection.describe = Mock(
            return_value={
                'sobjects': [
                    {
                        'name': 'Account',
                        'retrieveable': True,
                        'queryable': True
                    }
                ]
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
            'operation': [
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
            (result, errors) = loader.load_extraction_operation(ex, context)
        
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
        context.steps = []
        context.connection.describe = Mock(
            return_value={
                'sobjects': [
                    {
                        'name': 'Account',
                        'retrieveable': True,
                        'queryable': True
                    }
                ]
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
            'operation': [
                { 
                    'sobject': 'Account',
                    'fields': [ 'Name', 'ParentId' ],
                    'extract': { 'all': True }
                }
            ]
        }

        m = unittest.mock.mock_open()
        with unittest.mock.patch('builtins.open', m):
            (result, errors) = loader.load_extraction_operation(ex, context)

        m.assert_not_called()

        self.assertIsNone(result)
        self.assertEqual(
            [
                'Field Account.ParentId does not exist or is not visible.'
            ],
            errors
        )

    def test_load_extraction_operation_creates_valid_steps(self):
        context = amaxa.ExtractOperation(Mock())
        context.connection.describe = Mock(
            return_value={
                'sobjects': [
                    {
                        'name': 'Account',
                        'retrieveable': True,
                        'queryable': True
                    },
                    {
                        'name': 'Contact',
                        'retrieveable': True,
                        'queryable': True
                    },
                    {
                        'name': 'Opportunity',
                        'retrieveable': True,
                        'queryable': True
                    },
                    {
                        'name': 'Task',
                        'retrieveable': True,
                        'queryable': True
                    }
                ]
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
            'operation': [
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
                },
                {
                    'sobject': 'Opportunity',
                    'fields': [ 'Name' ],
                    'extract': {
                        'descendents': True
                    }
                },
                {
                    'sobject': 'Task',
                    'fields': [ 'Name' ],
                    'extract': {
                        'query': 'AccountId != null'
                    }
                }

            ]
        }

        m = unittest.mock.mock_open()
        with unittest.mock.patch('builtins.open', m):
            (result, errors) = loader.load_extraction_operation(ex, context)
            
        m.assert_has_calls(
            [
                unittest.mock.call('Account.csv', 'w'),
                unittest.mock.call('Contact.csv', 'w'),
                unittest.mock.call('Opportunity.csv', 'w'),
                unittest.mock.call('Task.csv', 'w')
            ],
            any_order=True
        )

        context.add_dependency.assert_has_calls(
            [
                unittest.mock.call('Contact', amaxa.SalesforceId('003000000000000')),
                unittest.mock.call('Contact', amaxa.SalesforceId('003000000000001'))
            ]
        )

        self.assertEqual([], errors)
        self.assertIsInstance(result, amaxa.ExtractOperation)
        self.assertEqual(4, len(result.steps))
        self.assertEqual('Account', result.steps[0].sobjectname)
        self.assertEqual(amaxa.ExtractionScope.ALL_RECORDS, result.steps[0].scope)
        self.assertEqual('Contact', result.steps[1].sobjectname)
        self.assertEqual(amaxa.ExtractionScope.SELECTED_RECORDS, result.steps[1].scope)
        self.assertEqual('Opportunity', result.steps[2].sobjectname)
        self.assertEqual(amaxa.ExtractionScope.DESCENDENTS, result.steps[2].scope)
        self.assertEqual('Task', result.steps[3].sobjectname)
        self.assertEqual(amaxa.ExtractionScope.QUERY, result.steps[3].scope)

    def test_load_extraction_operation_finds_readable_field_group(self):
        context = amaxa.ExtractOperation(Mock())
        context.connection.describe = Mock(
            return_value={
                'sobjects': [
                    {
                        'name': 'Account',
                        'retrieveable': True,
                        'queryable': True
                    }
                ]
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
            'operation': [
                { 
                    'sobject': 'Account',
                    'field-group': 'readable',
                    'extract': { 'all': True }
                }
            ]
        }

        m = unittest.mock.mock_open()
        with unittest.mock.patch('builtins.open', m):
            (result, errors) = loader.load_extraction_operation(ex, context)

        self.assertEqual([], errors)
        self.assertIsInstance(result, amaxa.ExtractOperation)
        m.assert_called_once_with('Account.csv', 'w')

        self.assertEqual({'Name', 'Id', 'Industry'}, result.steps[0].field_scope)

    def test_load_extraction_operation_finds_writeable_field_group(self):
        context = amaxa.ExtractOperation(Mock())
        context.connection.describe = Mock(
            return_value={
                'sobjects': [
                    {
                        'name': 'Account',
                        'retrieveable': True,
                        'queryable': True
                    }
                ]
            }
        )
        context.get_field_map = Mock(
            return_value={ 
                'Name': {
                    'type': 'string',
                    'createable': True
                },
                'Id': {
                    'type': 'string',
                    'createable': True
                },
                'Industry': {
                    'type': 'string',
                    'createable': False
                }
            }
        )

        ex = {
            'version': 1,
            'operation': [
                { 
                    'sobject': 'Account',
                    'field-group': 'writeable',
                    'extract': { 'all': True }
                }
            ]
        }

        m = unittest.mock.mock_open()
        with unittest.mock.patch('builtins.open', m):
            (result, errors) = loader.load_extraction_operation(ex, context)

        self.assertEqual([], errors)
        self.assertIsInstance(result, amaxa.ExtractOperation)
        m.assert_called_once_with('Account.csv', 'w')

        self.assertEqual(1, len(result.steps))
        self.assertEqual({'Name', 'Id'}, result.steps[0].field_scope)

    def test_load_extraction_operation_readable_field_group_omits_unsupported_types(self):
        context = amaxa.ExtractOperation(Mock())
        context.connection.describe = Mock(
            return_value={
                'sobjects': [
                    {
                        'name': 'Account',
                        'retrieveable': True,
                        'createable': True,
                        'queryable': True
                    }
                ]
            }
        )
        context.get_field_map = Mock(
            return_value={ 
                'Name': {
                    'type': 'string',
                    'createable': True
                },
                'Id': {
                    'type': 'string',
                    'createable': False
                },
                'MailingAddress': {
                    'type': 'address',
                    'createable': False
                },
                'Geolocation__c': {
                    'type': 'location',
                    'createable': False
                },
                'Body': {
                    'type': 'base64',
                    'createable': True
                }
            }
        )

        ex = {
            'version': 1,
            'operation': [
                { 
                    'sobject': 'Account',
                    'field-group': 'readable',
                    'extract': { 'all': True }
                }
            ]
        }

        m = unittest.mock.mock_open()
        with unittest.mock.patch('builtins.open', m):
            (result, errors) = loader.load_extraction_operation(ex, context)

        self.assertEqual([], errors)
        self.assertIsInstance(result, amaxa.ExtractOperation)
        m.assert_called_once_with('Account.csv', 'w')

        self.assertEqual(1, len(result.steps))
        self.assertEqual({'Id', 'Name'}, result.steps[0].field_scope)

    def test_load_extraction_operation_writeable_field_group_omits_unsupported_types(self):
        context = amaxa.ExtractOperation(Mock())
        context.connection.describe = Mock(
            return_value={
                'sobjects': [
                    {
                        'name': 'Account',
                        'retrieveable': True,
                        'createable': True,
                        'queryable': True
                    }
                ]
            }
        )
        context.get_field_map = Mock(
            return_value={ 
                'Name': {
                    'type': 'string',
                    'createable': True
                },
                'Id': {
                    'type': 'string',
                    'createable': False
                },
                'MailingAddress': {
                    'type': 'address',
                    'createable': False
                },
                'Geolocation__c': {
                    'type': 'location',
                    'createable': False
                },
                'Body': {
                    'type': 'base64',
                    'createable': True
                }
            }
        )

        ex = {
            'version': 1,
            'operation': [
                { 
                    'sobject': 'Account',
                    'field-group': 'writeable',
                    'extract': { 'all': True }
                }
            ]
        }

        m = unittest.mock.mock_open()
        with unittest.mock.patch('builtins.open', m):
            (result, errors) = loader.load_extraction_operation(ex, context)

        self.assertEqual([], errors)
        self.assertIsInstance(result, amaxa.ExtractOperation)
        m.assert_called_once_with('Account.csv', 'w')

        self.assertEqual(1, len(result.steps))
        self.assertEqual({'Id', 'Name'}, result.steps[0].field_scope)

    def test_load_extraction_operation_generates_field_list(self):
        connection = Mock()
        context = amaxa.ExtractOperation(connection)
        context.connection.describe = Mock(
            return_value={
                'sobjects': [
                    {
                        'name': 'Account',
                        'retrieveable': True,
                        'queryable': True
                    }
                ]
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
            'operation': [
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
            (result, errors) = loader.load_extraction_operation(ex, context)

        self.assertEqual([], errors)
        self.assertIsInstance(result, amaxa.ExtractOperation)
        m.assert_called_once_with('Account.csv', 'w')

        self.assertEqual({'Name', 'Industry', 'Id'}, result.steps[0].field_scope)

    def test_load_extraction_operation_creates_export_mapper(self):
        context = amaxa.ExtractOperation(Mock())
        context.connection.describe = Mock(
            return_value={
                'sobjects': [
                    {
                        'name': 'Account',
                        'retrieveable': True,
                        'queryable': True
                    }
                ]
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
            'operation': [
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
            (result, errors) = loader.load_extraction_operation(ex, context)

        self.assertEqual([], errors)
        self.assertIsInstance(result, amaxa.ExtractOperation)
        m.assert_called_once_with('Account.csv', 'w')

        self.assertEqual({'Name', 'Industry', 'Id'}, result.steps[0].field_scope)
        self.assertIn('Account', context.mappers)

        mapper = context.mappers['Account']
        self.assertEqual(
            {'Account Name': 'university of caprica', 'Industry': 'Education'},
            mapper.transform_record({ 'Name': 'UNIversity of caprica  ', 'Industry': 'Education' })
        )

    def test_load_extraction_operation_raises_exception_base64_fields(self):
        connection = Mock()
        context = amaxa.ExtractOperation(connection)
        context.connection.describe = Mock(
            return_value={
                'sobjects': [
                    {
                        'name': 'Account',
                        'retrieveable': True,
                        'queryable': True
                    }
                ]
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
                'Body': {
                    'type': 'base64'
                }
            }
        )

        ex = {
            'version': 1,
            'operation': [
                { 
                    'sobject': 'Account',
                    'fields': [
                        'Name', 
                        'Body'
                    ],
                    'extract': { 'all': True }
                }
            ]
        }

        m = unittest.mock.mock_open()
        with unittest.mock.patch('builtins.open', m):
            (result, errors) = loader.load_extraction_operation(ex, context)

        self.assertEqual(['Field {}.{} is a base64 field, which is not supported.'.format('Account', 'Body')], errors)
        self.assertIsNone(result)
        m.assert_not_called()

    def test_load_extraction_operation_catches_duplicate_columns(self):
        context = amaxa.ExtractOperation(Mock())
        context.connection.describe = Mock(
            return_value={
                'sobjects': [
                    {
                        'name': 'Account',
                        'retrieveable': True,
                        'queryable': True
                    }
                ]
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
            'operation': [
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
            (result, errors) = loader.load_extraction_operation(ex, context)

        self.assertIsNone(result)
        self.assertEqual(
            ['Field Account.Industry is mapped to column Industry, but this column is already mapped.',
            'Field Account.AccountSite is mapped to column Industry, but this column is already mapped.'],
            errors
        )

    def test_load_extraction_operation_catches_duplicate_fields(self):
        context = amaxa.ExtractOperation(Mock())
        context.connection.describe = Mock(
            return_value={
                'sobjects': [
                    {
                        'name': 'Account',
                        'retrieveable': True,
                        'queryable': True
                    }
                ]
            }
        )
        context.get_field_map = Mock(
            return_value={ 
                'Name': {
                    'type': 'string',
                    'createable': True
                },
                'Id': {
                    'type': 'string',
                    'createable': False
                },
                'Industry': {
                    'type': 'string',
                    'createable': True
                }
            }
        )

        ex = {
            'version': 1,
            'operation': [
                { 
                    'sobject': 'Account',
                    'fields': [
                        {
                            'field': 'Name',
                            'column': 'Industry',
                        },
                        {
                            'field': 'Name',
                            'column': 'Name'
                        }
                    ],
                    'extract': { 'all': True }
                }
            ]
        }

        m = unittest.mock.mock_open()
        with unittest.mock.patch('builtins.open', m):
            (result, errors) = loader.load_extraction_operation(ex, context)

        self.assertIsNone(result)
        self.assertEqual(
            ['Field Account.Name is present more than once in the specification.'],
            errors
        )

    def test_load_extraction_operation_populates_lookup_behaviors(self):
        context = amaxa.ExtractOperation(Mock())
        context.connection.describe = Mock(
            return_value={
                'sobjects': [
                    {
                        'name': 'Account',
                        'retrieveable': True,
                        'queryable': True
                    },
                    {
                        'name': 'Contact',
                        'retrieveable': True,
                        'queryable': True
                    }

                ]
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
            'operation': [
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
            (result, errors) = loader.load_extraction_operation(ex, context)

        self.assertEqual([], errors)
        self.assertIsInstance(result, amaxa.ExtractOperation)

        self.assertEqual(amaxa.SelfLookupBehavior.TRACE_NONE, result.steps[0].get_self_lookup_behavior_for_field('ParentId'))
        self.assertEqual(amaxa.OutsideLookupBehavior.DROP_FIELD, result.steps[0].get_outside_lookup_behavior_for_field('Primary_Contact__c'))

    def test_load_extraction_operation_validates_lookup_behaviors_for_self_lookups(self):
        context = amaxa.ExtractOperation(Mock())
        context.connection.describe = Mock(
            return_value={
                'sobjects': [
                    {
                        'name': 'Account',
                        'retrieveable': True,
                        'createable': True,
                        'queryable': True
                    }
                ]
            }
        )
        context.get_field_map = Mock(
            return_value={ 
                'Name': {
                    'type': 'string',
                    'createable': True
                },
                'Id': {
                    'type': 'string',
                    'createable': False
                },
                'ParentId': {
                    'type': 'reference',
                    'referenceTo': ['Account'],
                    'createable': True
                }
            }
        )

        ex = {
            'version': 1,
            'operation': [
                { 
                    'sobject': 'Account',
                    'fields': [
                        'Name',
                        {
                            'field': 'ParentId',
                            'outside-lookup-behavior': 'include'
                        }
                    ],
                    'extract': { 'all': True }
                }
            ]
        }

        m = unittest.mock.mock_open()
        with unittest.mock.patch('builtins.open', m):
            (result, errors) = loader.load_extraction_operation(ex, context)

        self.assertEqual(
            [
                'Lookup behavior \'{}\' specified for field {}.{} is not valid for this lookup type.'.format(
                    'include',
                    'Account',
                    'ParentId'
                )
            ],
            errors
        )
        self.assertIsNone(result)

    def test_load_extraction_operation_validates_lookup_behaviors_for_dependent_lookups(self):
        context = amaxa.ExtractOperation(Mock())
        context.connection.describe = Mock(
            return_value={
                'sobjects': [
                    {
                        'name': 'Account',
                        'retrieveable': True,
                        'createable': True,
                        'queryable': True
                    },
                    {
                        'name': 'Contact',
                        'retrieveable': True,
                        'createable': True,
                        'queryable': True
                    }
                ]
            }
        )
        context.get_field_map = Mock(
            return_value={ 
                'Name': {
                    'type': 'string',
                    'createable': True
                },
                'Id': {
                    'type': 'string',
                    'createable': False
                },
                'Primary_Contact__c': {
                    'type': 'reference',
                    'referenceTo': ['Contact'],
                    'createable': True
                }
            }
        )

        ex = {
            'version': 1,
            'operation': [
                { 
                    'sobject': 'Account',
                    'fields': [
                        'Name',
                        {
                            'field': 'Primary_Contact__c',
                            'self-lookup-behavior': 'trace-all'
                        }
                    ],
                    'extract': { 'all': True }
                },
                { 
                    'sobject': 'Contact',
                    'fields': [ 'Name' ],
                    'extract': { 'all': True }
                },
            ]
        }

        m = unittest.mock.mock_open()
        with unittest.mock.patch('builtins.open', m):
            (result, errors) = loader.load_extraction_operation(ex, context)

        self.assertEqual(
            [
                'Lookup behavior \'{}\' specified for field {}.{} is not valid for this lookup type.'.format(
                    'trace-all',
                    'Account',
                    'Primary_Contact__c'
                )
            ],
            errors
        )
        self.assertIsNone(result)

    @unittest.mock.patch('logging.getLogger')
    def test_load_extraction_operation_warns_lookups_other_objects(self, logger):
        context = amaxa.ExtractOperation(Mock())
        amaxa_logger = Mock()
        logger.return_value=amaxa_logger
        context.connection.describe = Mock(
            return_value={
                'sobjects': [
                    {
                        'name': 'Account',
                        'retrieveable': True,
                        'queryable': True
                    },
                    {
                        'name': 'Contact',
                        'retrieveable': True,
                        'queryable': True
                    },
                    {
                        'name': 'Test__c',
                        'retrieveable': True,
                        'queryable': True
                    }
                ]
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
            'operation': [
                { 
                    'sobject': 'Test__c',
                    'fields': [ 'Parent__c' ],
                    'extract': { 'all': True }
                }
            ]
        }

        m = unittest.mock.mock_open()
        with unittest.mock.patch('builtins.open', m):
            (result, errors) = loader.load_extraction_operation(ex, context)

        self.assertEqual([], errors)
        self.assertIsInstance(result, amaxa.ExtractOperation)
        amaxa_logger.warn.assert_called_once_with(
            'Field %s.%s is a reference whose targets (%s) are not all included in the extraction. Reference handlers will be inactive for references to non-included sObjects.',
            'Test__c',
            'Parent__c',
            ', '.join(['Parent__c'])
        )


class test_load_load_operation(unittest.TestCase):
    def test_load_load_operation_returns_validation_errors(self):
        context = Mock()
        (result, errors) = loader.load_load_operation(
            {
                'operation': [
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
    def test_load_load_operation_traps_login_exceptions(self, sf_mock):
        return_exception = simple_salesforce.SalesforceAuthenticationFailed(500, 'Internal Server Error')
        sf_mock.describe = Mock(side_effect=return_exception)
        context = amaxa.LoadOperation(sf_mock)
        ex = {
            'version': 1,
            'operation': [
                { 
                    'sobject': 'Account',
                    'field-group': 'writeable',
                    'extract': { 'all': True }
                }
            ]
        }

        (result, errors) = loader.load_load_operation(ex, context)

        self.assertIsNone(result)
        self.assertEqual(['Unable to authenticate to Salesforce: {}'.format(return_exception)], errors)

    def test_load_load_operation_flags_missing_sobjects(self):
        context = Mock()
        context.steps = []
        context.connection.describe = Mock(
            return_value={
                'sobjects': [
                    {
                        'name': 'Account',
                        'createable': False
                    }
                ]
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
            'operation': [
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
            (result, errors) = loader.load_load_operation(ex, context)
        
        m.assert_not_called()

        self.assertIsNone(result)
        self.assertEqual(
            [
                'sObject Account does not exist, is not visible, or is not createable.',
                'sObject Contact does not exist, is not visible, or is not createable.',
                'sObject Opportunity does not exist, is not visible, or is not createable.'
            ],
            errors
        )
    def test_validate_load_flags_missing_fields(self):
        context = Mock()
        context.steps = []
        context.connection.describe = Mock(
            return_value={
                'sobjects': [
                    {
                        'name': 'Account',
                        'createable': True
                    }
                ]
            }
        )
        context.get_field_map = Mock(
            return_value={ 
                'Name': {
                    'type': 'string',
                    'createable': True
                },
                'Id': {
                    'type': 'string',
                    'createable': True
                }
            }
        )

        ex = { 
            'version': 1, 
            'operation': [
                { 
                    'sobject': 'Account',
                    'fields': [ 'Name', 'ParentId' ],
                    'extract': { 'all': True }
                }
            ]
        }

        m = unittest.mock.mock_open()
        with unittest.mock.patch('builtins.open', m):
            (result, errors) = loader.load_load_operation(ex, context)

        m.assert_not_called()

        self.assertIsNone(result)
        self.assertEqual(
            [
                'Field Account.ParentId does not exist, is not writeable, or is not visible.'
            ],
            errors
        )

    def test_validate_load_flags_non_writeable_fields(self):
        context = Mock()
        context.steps = []
        context.connection.describe = Mock(
            return_value={
                'sobjects': [
                    {
                        'name': 'Account',
                        'createable': True
                    }
                ]
            }
        )
        context.get_field_map = Mock(
            return_value={ 
                'Name': {
                    'type': 'string',
                    'createable': True
                },
                'Id': {
                    'type': 'string',
                    'createable': True
                },
                'Industry': {
                    'type': 'string',
                    'createable': False
                }
            }
        )

        ex = { 
            'version': 1, 
            'operation': [
                { 
                    'sobject': 'Account',
                    'fields': [ 'Name', 'Industry' ],
                    'extract': { 'all': True }
                }
            ]
        }

        m = unittest.mock.mock_open()
        with unittest.mock.patch('builtins.open', m):
            (result, errors) = loader.load_load_operation(ex, context)

        m.assert_not_called()

        self.assertIsNone(result)
        self.assertEqual(
            [
                'Field Account.Industry does not exist, is not writeable, or is not visible.'
            ],
            errors
        )

    def test_validate_load_flags_non_updateable_dependent_fields(self):
        context = amaxa.LoadOperation(Mock())
        context.connection.describe = Mock(
            return_value={
                'sobjects': [
                    {
                        'name': 'Account',
                        'createable': True
                    }
                ]
            }
        )
        context.get_field_map = Mock(
            return_value={ 
                'Name': {
                    'type': 'string',
                    'createable': True
                },
                'Id': {
                    'type': 'string',
                    'createable': True
                },
                'ParentId': {
                    'type': 'reference',
                    'referenceTo': ['Account'],
                    'createable': True,
                    'updateable': False
                }
            }
        )

        ex = { 
            'version': 1, 
            'operation': [
                { 
                    'sobject': 'Account',
                    'fields': [ 'Name', 'ParentId' ],
                    'extract': { 'all': True }
                }
            ]
        }

        m = unittest.mock.mock_open()
        with unittest.mock.patch('builtins.open', m):
            (result, errors) = loader.load_load_operation(ex, context)

        m.assert_not_called()

        self.assertEqual(
            [
                'Field {}.{} is a dependent lookup, but is not updateable.'.format('Account', 'ParentId')
            ],
            errors
        )
        self.assertIsNone(result)

    def test_load_load_operation_creates_valid_steps(self):
        context = amaxa.LoadOperation(Mock())
        context.connection.describe = Mock(
            return_value={
                'sobjects': [
                    {
                        'name': 'Account',
                        'retrieveable': True,
                        'createable': True,
                        'queryable': True
                    },
                    {
                        'name': 'Contact',
                        'retrieveable': True,
                        'createable': True,
                        'queryable': True
                    },
                    {
                        'name': 'Opportunity',
                        'retrieveable': True,
                        'createable': True,
                        'queryable': True
                    },
                    {
                        'name': 'Task',
                        'retrieveable': True,
                        'createable': True,
                        'queryable': True
                    }
                ]
            }
        )
        context.get_field_map = Mock(
            return_value={ 
                'Name': {
                    'type': 'string',
                    'createable': True
                },
                'LastName': {
                    'type': 'string',
                    'createable': True
                },
                'StageName': {
                    'type': 'string',
                    'createable': True
                },
                'Subject': {
                    'type': 'string',
                    'createable': True
                },
                'Id': {
                    'type': 'string',
                    'createable': False
                }
            }
        )
        context.add_dependency = Mock()

        ex = {
            'version': 1,
            'operation': [
                { 
                    'sobject': 'Account',
                    'fields': [ 'Name' ],
                    'extract': { 'all': True },
                    'input-validation': 'none'
                },
                { 
                    'sobject': 'Contact',
                    'fields': [ 'LastName' ],
                    'extract': { 'all': True },
                    'input-validation': 'none'
                },
                {
                    'sobject': 'Opportunity',
                    'fields': [ 'StageName' ],
                    'extract': { 'all': True },
                    'input-validation': 'none'
                },
                {
                    'sobject': 'Task',
                    'fields': [ 'Subject' ],
                    'extract': { 'all': True },
                    'input-validation': 'none'
                }

            ]
        }

        m = unittest.mock.mock_open()
        with unittest.mock.patch('builtins.open', m):
            (result, errors) = loader.load_load_operation(ex, context)

        self.assertEqual([], errors)
        self.assertIsInstance(result, amaxa.LoadOperation)

        m.assert_has_calls(
            [
                unittest.mock.call('Account.csv', 'r'),
                unittest.mock.call('Contact.csv', 'r'),
                unittest.mock.call('Opportunity.csv', 'r'),
                unittest.mock.call('Task.csv', 'r')
            ],
            any_order=True
        )

        self.assertEqual(4, len(result.steps))
        self.assertEqual('Account', result.steps[0].sobjectname)
        self.assertEqual('Contact', result.steps[1].sobjectname)
        self.assertEqual('Opportunity', result.steps[2].sobjectname)
        self.assertEqual('Task', result.steps[3].sobjectname)

    def test_load_load_operation_finds_writeable_field_group(self):
        context = amaxa.LoadOperation(Mock())
        context.connection.describe = Mock(
            return_value={
                'sobjects': [
                    {
                        'name': 'Account',
                        'retrieveable': True,
                        'createable': True,
                        'queryable': True
                    }
                ]
            }
        )
        context.get_field_map = Mock(
            return_value={ 
                'Name': {
                    'type': 'string',
                    'createable': False
                },
                'Id': {
                    'type': 'string',
                    'createable': False
                },
                'Industry': {
                    'type': 'string',
                    'createable': True
                }
            }
        )

        ex = {
            'version': 1,
            'operation': [
                { 
                    'sobject': 'Account',
                    'field-group': 'writeable',
                    'extract': { 'all': True },
                    'input-validation': 'none'
                }
            ]
        }

        m = unittest.mock.mock_open()
        with unittest.mock.patch('builtins.open', m):
            (result, errors) = loader.load_load_operation(ex, context)

        self.assertEqual([], errors)
        self.assertIsInstance(result, amaxa.LoadOperation)
        m.assert_called_once_with('Account.csv', 'r')

        self.assertEqual(1, len(result.steps))
        self.assertEqual({'Industry'}, result.steps[0].field_scope)

    def test_load_load_operation_field_groups_omit_unsupported_types(self):
        context = amaxa.LoadOperation(Mock())
        context.connection.describe = Mock(
            return_value={
                'sobjects': [
                    {
                        'name': 'Account',
                        'retrieveable': True,
                        'createable': True,
                        'queryable': True
                    }
                ]
            }
        )
        context.get_field_map = Mock(
            return_value={ 
                'Name': {
                    'type': 'string',
                    'createable': True
                },
                'Body': {
                    'type': 'base64',
                    'createable': True
                }
            }
        )

        ex = {
            'version': 1,
            'operation': [
                { 
                    'sobject': 'Account',
                    'field-group': 'writeable',
                    'extract': { 'all': True },
                    'input-validation': 'none'
                }
            ]
        }

        m = unittest.mock.mock_open()
        with unittest.mock.patch('builtins.open', m):
            (result, errors) = loader.load_load_operation(ex, context)

        self.assertEqual([], errors)
        self.assertIsInstance(result, amaxa.LoadOperation)
        m.assert_called_once_with('Account.csv', 'r')

        self.assertEqual(1, len(result.steps))
        self.assertEqual({'Name'}, result.steps[0].field_scope)

    def test_load_load_operation_generates_field_list(self):
        connection = Mock()
        context = amaxa.LoadOperation(connection)
        context.connection.describe = Mock(
            return_value={
                'sobjects': [
                    {
                        'name': 'Account',
                        'retrieveable': True,
                        'createable': True,
                        'queryable': True
                    }
                ]
            }
        )
        context.get_field_map = Mock(
            return_value={ 
                'Name': {
                    'type': 'string',
                    'createable': True
                },
                'Id': {
                    'type': 'string',
                    'createable': False
                },
                'Industry': {
                    'type': 'string',
                    'createable': True
                }
            }
        )

        ex = {
            'version': 1,
            'operation': [
                { 
                    'sobject': 'Account',
                    'fields': [
                        'Name', 
                        'Industry'
                    ],
                    'extract': { 'all': True },
                    'input-validation': 'none'
                }
            ]
        }

        m = unittest.mock.mock_open()
        with unittest.mock.patch('builtins.open', m):
            (result, errors) = loader.load_load_operation(ex, context)
            

        self.assertEqual([], errors)
        self.assertIsInstance(result, amaxa.LoadOperation)
        m.assert_called_once_with('Account.csv', 'r')

        self.assertEqual({'Name', 'Industry'}, result.steps[0].field_scope)

    def test_load_load_operation_respects_none_validation_option(self):
        connection = Mock()
        context = amaxa.LoadOperation(connection)
        context.connection.describe = Mock(
            return_value={
                'sobjects': [
                    {
                        'name': 'Account',
                        'retrieveable': True,
                        'createable': True,
                        'queryable': True
                    }
                ]
            }
        )
        context.get_field_map = Mock(
            return_value={ 
                'Name': {
                    'type': 'string',
                    'createable': True
                },
                'Id': {
                    'type': 'string',
                    'createable': False
                },
                'Industry': {
                    'type': 'string',
                    'createable': True
                }
            }
        )

        ex = {
            'version': 1,
            'operation': [
                { 
                    'sobject': 'Account',
                    'fields': [
                        'Name', 
                        'Industry'
                    ],
                    'extract': { 'all': True },
                    'input-validation': 'none'
                }
            ]
        }

        open_mock = Mock(side_effect=io.StringIO('Id,Name,Industry,Description'))
        with unittest.mock.patch('builtins.open', open_mock):
            (result, errors) = loader.load_load_operation(ex, context)

        self.assertEqual([], errors)
        self.assertIsInstance(result, amaxa.LoadOperation)
        open_mock.assert_called_once_with('Account.csv', 'r')

        self.assertEqual({'Name', 'Industry'}, result.steps[0].field_scope)

    def test_load_load_operation_validates_file_against_field_scope_excess_fields(self):
        connection = Mock()
        context = amaxa.LoadOperation(connection)
        context.connection.describe = Mock(
            return_value={
                'sobjects': [
                    {
                        'name': 'Account',
                        'retrieveable': True,
                        'createable': True,
                        'queryable': True
                    }
                ]
            }
        )
        context.get_field_map = Mock(
            return_value={ 
                'Name': {
                    'type': 'string',
                    'createable': True
                },
                'Id': {
                    'type': 'string',
                    'createable': False
                },
                'Industry': {
                    'type': 'string',
                    'createable': True
                }
            }
        )

        ex = {
            'version': 1,
            'operation': [
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

        fieldnames = ['Id', 'Name', 'Industry', 'Description']
        account_mock = Mock(return_value=io.StringIO(','.join(fieldnames)))
        with unittest.mock.patch('builtins.open', account_mock):
            (result, errors) = loader.load_load_operation(ex, context)

        self.assertEqual(
            [
                'Input file for sObject {} does not match specified field scope.\nScope: {}\nFile Columns: {}\n'.format(
                    'Account',
                    ', '.join(sorted(['Name', 'Industry'])),
                    ', '.join(sorted(set(fieldnames) - set(['Id'])))
                )
            ],
            errors
         )
        self.assertIsNone(result)
        account_mock.assert_called_once_with('Account.csv', 'r')

    def test_load_load_operation_validates_file_against_field_scope_missing_fields(self):
        connection = Mock()
        context = amaxa.LoadOperation(connection)
        context.connection.describe = Mock(
            return_value={
                'sobjects': [
                    {
                        'name': 'Account',
                        'retrieveable': True,
                        'createable': True,
                        'queryable': True
                    }
                ]
            }
        )
        context.get_field_map = Mock(
            return_value={ 
                'Name': {
                    'type': 'string',
                    'createable': True
                },
                'Id': {
                    'type': 'string',
                    'createable': False
                },
                'Industry': {
                    'type': 'string',
                    'createable': True
                }
            }
        )

        ex = {
            'version': 1,
            'operation': [
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

        fieldnames = ['Id', 'Name']
        account_mock = Mock(return_value=io.StringIO(','.join(fieldnames)))
        with unittest.mock.patch('builtins.open', account_mock):
            (result, errors) = loader.load_load_operation(ex, context)

        self.assertEqual(
            [
                'Input file for sObject {} does not match specified field scope.\nScope: {}\nFile Columns: {}\n'.format(
                    'Account',
                    ', '.join(sorted(['Name', 'Industry'])),
                    ', '.join(sorted(set(fieldnames) - set(['Id'])))
                )
            ],
            errors
         )
        self.assertIsNone(result)
        account_mock.assert_called_once_with('Account.csv', 'r')

    def test_load_load_operation_validates_file_against_field_group(self):
        connection = Mock()
        context = amaxa.LoadOperation(connection)
        context.connection.describe = Mock(
            return_value={
                'sobjects': [
                    {
                        'name': 'Account',
                        'retrieveable': True,
                        'createable': True,
                        'queryable': True
                    }
                ]
            }
        )
        context.get_field_map = Mock(
            return_value={ 
                'Name': {
                    'type': 'string',
                    'createable': True
                },
                'Id': {
                    'type': 'string',
                    'createable': False
                },
                'Industry': {
                    'type': 'string',
                    'createable': True
                }
            }
        )

        ex = {
            'version': 1,
            'operation': [
                { 
                    'sobject': 'Account',
                    'field-group': 'writeable',
                    'extract': { 'all': True }
                }
            ]
        }

        fieldnames = ['Id', 'Name', 'Industry', 'Description']
        account_mock = Mock(return_value=io.StringIO(','.join(fieldnames)))
        with unittest.mock.patch('builtins.open', account_mock):
            (result, errors) = loader.load_load_operation(ex, context)

        self.assertEqual(
            [
                'Input file for sObject {} contains excess columns over field group \'{}\': {}'.format(
                    'Account',
                    'writeable',
                    'Description'
                )
            ],
            errors
         )
        self.assertIsNone(result)
        account_mock.assert_called_once_with('Account.csv', 'r')

    def test_load_load_operation_validates_file_against_field_group_with_strict(self):
        connection = Mock()
        context = amaxa.LoadOperation(connection)
        context.connection.describe = Mock(
            return_value={
                'sobjects': [
                    {
                        'name': 'Account',
                        'retrieveable': True,
                        'createable': True,
                        'queryable': True
                    }
                ]
            }
        )
        context.get_field_map = Mock(
            return_value={ 
                'Name': {
                    'type': 'string',
                    'createable': True
                },
                'Id': {
                    'type': 'string',
                    'createable': False
                },
                'Industry': {
                    'type': 'string',
                    'createable': True
                }
            }
        )

        ex = {
            'version': 1,
            'operation': [
                { 
                    'sobject': 'Account',
                    'field-group': 'writeable',
                    'extract': { 'all': True },
                    'input-validation': 'strict'
                }
            ]
        }
        fieldnames = ['Id', 'Name']
        account_mock = Mock(return_value=io.StringIO(','.join(fieldnames) + '\n'))
        with unittest.mock.patch('builtins.open', account_mock):
            (result, errors) = loader.load_load_operation(ex, context)

        self.assertEqual(
            [
                'Input file for sObject {} does not match specified field scope.\nScope: {}\nFile Columns: {}\n'.format(
                    'Account',
                    ', '.join(sorted(['Name', 'Industry'])),
                    ', '.join(sorted(set(fieldnames) - set(['Id'])))
                )
            ],
            errors
         )
        self.assertIsNone(result)
        account_mock.assert_called_once_with('Account.csv', 'r')

    def test_load_load_operation_raises_exception_base64_fields(self):
        connection = Mock()
        context = amaxa.LoadOperation(connection)
        context.connection.describe = Mock(
            return_value={
                'sobjects': [
                    {
                        'name': 'Account',
                        'retrieveable': True,
                        'queryable': True,
                        'createable': True
                    }
                ]
            }
        )
        context.get_field_map = Mock(
            return_value={ 
                'Name': {
                    'type': 'string',
                    'createable': True
                },
                'Id': {
                    'type': 'string',
                    'createable': True
                },
                'Body': {
                    'type': 'base64',
                    'createable': True
                }
            }
        )

        ex = {
            'version': 1,
            'operation': [
                { 
                    'sobject': 'Account',
                    'fields': [
                        'Name', 
                        'Body'
                    ],
                    'extract': { 'all': True }
                }
            ]
        }

        m = unittest.mock.mock_open()
        with unittest.mock.patch('builtins.open', m):
            (result, errors) = loader.load_load_operation(ex, context)

        self.assertEqual(['Field {}.{} is a base64 field, which is not supported.'.format('Account', 'Body')], errors)
        self.assertIsNone(result)
        m.assert_not_called()

    def test_load_load_operation_creates_import_mapper(self):
        context = amaxa.LoadOperation(Mock())
        context.connection.describe = Mock(
            return_value={
                'sobjects': [
                    {
                        'name': 'Account',
                        'retrieveable': True,
                        'createable': True,
                        'queryable': True
                    }
                ]
            }
        )
        context.get_field_map = Mock(
            return_value={ 
                'Name': {
                    'type': 'string',
                    'createable': True
                },
                'Id': {
                    'type': 'string',
                    'createable': False
                },
                'Industry': {
                    'type': 'string',
                    'createable': True
                }
            }
        )

        ex = {
            'version': 1,
            'operation': [
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
                    'extract': { 'all': True },
                    'input-validation': 'none'
                }
            ]
        }

        m = unittest.mock.mock_open()
        with unittest.mock.patch('builtins.open', m):
            (result, errors) = loader.load_load_operation(ex, context)

        self.assertEqual([], errors)
        self.assertIsInstance(result, amaxa.LoadOperation)
        m.assert_called_once_with('Account.csv', 'r')

        self.assertEqual({'Name', 'Industry'}, result.steps[0].field_scope)
        self.assertIn('Account', context.mappers)

        mapper = context.mappers['Account']
        self.assertEqual({'Account Name': 'Name'}, mapper.field_name_mapping)
        self.assertEqual(
            {'Name': 'university of caprica', 'Industry': 'Education'},
            mapper.transform_record({ 'Account Name': 'UNIversity of caprica  ', 'Industry': 'Education' })
        )

    def test_load_load_operation_catches_duplicate_columns(self):
        context = amaxa.LoadOperation(Mock())
        context.connection.describe = Mock(
            return_value={
                'sobjects': [
                    {
                        'name': 'Account',
                        'retrieveable': True,
                        'createable': True,
                        'queryable': True
                    }
                ]
            }
        )
        context.get_field_map = Mock(
            return_value={ 
                'Name': {
                    'type': 'string',
                    'createable': True
                },
                'Id': {
                    'type': 'string',
                    'createable': False
                },
                'Industry': {
                    'type': 'string',
                    'createable': True
                },
                'AccountSite': {
                    'type': 'string',
                    'createable': True
                }
            }
        )

        ex = {
            'version': 1,
            'operation': [
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
            (result, errors) = loader.load_load_operation(ex, context)

        self.assertIsNone(result)
        self.assertEqual(
            ['Column Industry is mapped to field Account.Industry, but this column is already mapped.',
            'Column Industry is mapped to field Account.AccountSite, but this column is already mapped.'],
            errors
        )

    def test_load_load_operation_catches_duplicate_fields(self):
        context = amaxa.LoadOperation(Mock())
        context.connection.describe = Mock(
            return_value={
                'sobjects': [
                    {
                        'name': 'Account',
                        'retrieveable': True,
                        'createable': True,
                        'queryable': True
                    }
                ]
            }
        )
        context.get_field_map = Mock(
            return_value={ 
                'Name': {
                    'type': 'string',
                    'createable': True
                },
                'Id': {
                    'type': 'string',
                    'createable': False
                },
                'Industry': {
                    'type': 'string',
                    'createable': True
                }
            }
        )

        ex = {
            'version': 1,
            'operation': [
                { 
                    'sobject': 'Account',
                    'fields': [
                        {
                            'field': 'Name',
                            'column': 'Industry',
                        },
                        {
                            'field': 'Name',
                            'column': 'Name'
                        }
                    ],
                    'extract': { 'all': True }
                }
            ]
        }

        m = unittest.mock.mock_open()
        with unittest.mock.patch('builtins.open', m):
            (result, errors) = loader.load_load_operation(ex, context)

        self.assertIsNone(result)
        self.assertEqual(
            ['Field Account.Name is present more than once in the specification.'],
            errors
        )

    def test_load_load_operation_populates_lookup_behaviors(self):
        context = amaxa.LoadOperation(Mock())
        context.connection.describe = Mock(
            return_value={
                'sobjects': [
                    {
                        'name': 'Account',
                        'retrieveable': True,
                        'createable': True,
                        'queryable': True
                    },
                    {
                        'name': 'Contact',
                        'retrieveable': True,
                        'createable': True,
                        'queryable': True
                    }

                ]
            }
        )
        context.get_field_map = Mock(
            return_value={ 
                'Name': {
                    'type': 'string',
                    'createable': True
                },
                'Id': {
                    'type': 'string',
                    'createable': False
                },
                'ParentId': {
                    'type': 'reference',
                    'referenceTo': ['Account'],
                    'createable': True,
                    'updateable': True
                },
                'Primary_Contact__c': {
                    'type': 'reference',
                    'referenceTo': ['Contact'],
                    'createable': True,
                    'updateable': True
                }
            }
        )

        ex = {
            'version': 1,
            'operation': [
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
                    'extract': { 'all': True },
                    'input-validation': 'none'
                },
                {
                    'sobject': 'Contact',
                    'fields': [
                        {
                            'field': 'Name'
                        }
                    ],
                    'extract': { 'all': True },
                    'input-validation': 'none'
                }
            ]
        }

        m = unittest.mock.mock_open()
        with unittest.mock.patch('builtins.open', m):
            (result, errors) = loader.load_load_operation(ex, context)

        self.assertEqual([], errors)
        self.assertIsInstance(result, amaxa.LoadOperation)

        self.assertEqual(amaxa.SelfLookupBehavior.TRACE_NONE, result.steps[0].get_lookup_behavior_for_field('ParentId'))
        self.assertEqual(amaxa.OutsideLookupBehavior.DROP_FIELD, result.steps[0].get_lookup_behavior_for_field('Primary_Contact__c'))

    def test_load_load_operation_validates_lookup_behaviors_for_self_lookups(self):
        context = amaxa.LoadOperation(Mock())
        context.connection.describe = Mock(
            return_value={
                'sobjects': [
                    {
                        'name': 'Account',
                        'retrieveable': True,
                        'createable': True,
                        'queryable': True
                    }
                ]
            }
        )
        context.get_field_map = Mock(
            return_value={ 
                'Name': {
                    'type': 'string',
                    'createable': True
                },
                'Id': {
                    'type': 'string',
                    'createable': False
                },
                'ParentId': {
                    'type': 'reference',
                    'referenceTo': ['Account'],
                    'createable': True,
                    'updateable': True
                }
            }
        )

        ex = {
            'version': 1,
            'operation': [
                { 
                    'sobject': 'Account',
                    'fields': [
                        'Name',
                        {
                            'field': 'ParentId',
                            'outside-lookup-behavior': 'include'
                        }
                    ],
                    'extract': { 'all': True }
                }
            ]
        }

        m = unittest.mock.mock_open()
        with unittest.mock.patch('builtins.open', m):
            (result, errors) = loader.load_load_operation(ex, context)

        self.assertEqual(
            [
                'Lookup behavior \'{}\' specified for field {}.{} is not valid for this lookup type.'.format(
                    'include',
                    'Account',
                    'ParentId'
                )
            ],
            errors
        )
        self.assertIsNone(result)

    def test_load_load_operation_validates_lookup_behaviors_for_dependent_lookups(self):
        context = amaxa.LoadOperation(Mock())
        context.connection.describe = Mock(
            return_value={
                'sobjects': [
                    {
                        'name': 'Account',
                        'retrieveable': True,
                        'createable': True,
                        'queryable': True
                    },
                    {
                        'name': 'Contact',
                        'retrieveable': True,
                        'createable': True,
                        'queryable': True
                    }
                ]
            }
        )
        context.get_field_map = Mock(
            return_value={ 
                'Name': {
                    'type': 'string',
                    'createable': True
                },
                'Id': {
                    'type': 'string',
                    'createable': False
                },
                'Primary_Contact__c': {
                    'type': 'reference',
                    'referenceTo': ['Contact'],
                    'createable': True,
                    'updateable': True
                }
            }
        )

        ex = {
            'version': 1,
            'operation': [
                { 
                    'sobject': 'Account',
                    'fields': [
                        'Name',
                        {
                            'field': 'Primary_Contact__c',
                            'self-lookup-behavior': 'trace-all'
                        }
                    ],
                    'extract': { 'all': True }
                },
                { 
                    'sobject': 'Contact',
                    'fields': [ 'Name' ],
                    'extract': { 'all': True }
                },
            ]
        }

        m = unittest.mock.mock_open()
        with unittest.mock.patch('builtins.open', m):
            (result, errors) = loader.load_load_operation(ex, context)

        self.assertEqual(
            [
                'Lookup behavior \'{}\' specified for field {}.{} is not valid for this lookup type.'.format(
                    'trace-all',
                    'Account',
                    'Primary_Contact__c'
                )
            ],
            errors
        )
        self.assertIsNone(result)

    @unittest.mock.patch('logging.getLogger')
    def test_load_load_operation_warns_lookups_other_objects(self, logger):
        context = amaxa.LoadOperation(Mock())
        amaxa_logger = Mock()
        logger.return_value=amaxa_logger
        context.connection.describe = Mock(
            return_value={
                'sobjects': [
                    {
                        'name': 'Account',
                        'retrieveable': True,
                        'createable': True,
                        'queryable': True
                    },
                    {
                        'name': 'Contact',
                        'retrieveable': True,
                        'createable': True,
                        'queryable': True
                    },
                    {
                        'name': 'Test__c',
                        'retrieveable': True,
                        'createable': True,
                        'queryable': True
                    }
                ]
            }
        )
        context.get_field_map = Mock(
            return_value={ 
                'Id': {
                    'type': 'string',
                    'createable': False
                },
                'Parent__c': {
                    'type': 'reference',
                    'referenceTo': ['Parent__c'],
                    'createable': True
                }
            }
        )

        ex = {
            'version': 1,
            'operation': [
                { 
                    'sobject': 'Test__c',
                    'fields': [ 'Parent__c' ],
                    'extract': { 'all': True },
                    'input-validation': 'none'
                }
            ]
        }

        m = unittest.mock.mock_open()
        with unittest.mock.patch('builtins.open', m):
            (result, errors) = loader.load_load_operation(ex, context)

        self.assertEqual([], errors)
        self.assertIsInstance(result, amaxa.LoadOperation)
        amaxa_logger.warn.assert_called_once_with(
            'Field %s.%s is a reference whose targets (%s) are not all included in the load. Reference handlers will be inactive for references to non-included sObjects.',
            'Test__c',
            'Parent__c',
            ', '.join(['Parent__c'])
        )