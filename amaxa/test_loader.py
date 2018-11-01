import unittest
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
        self.assertEquals(['version: [\'required field\']'], errors)


    def test_validate_credential_schema_returns_normalized_input(self):
        credentials = {
            'version': 1,
            'credentials': {
                'username': 'baltar@ucaprica.edu',
                'password': '666666666'
            }
        }

        (result, errors) = loader.validate_credential_schema(credentials)

        self.assertEquals(False, result['credentials']['sandbox'])
        self.assertEquals([], errors)

    def test_validate_credential_schema_returns_errors(self):
        credentials = {
            'credentials': {
                'username': 'baltar@ucaprica.edu',
                'password': '666666666'
            }
        }

        (result, errors) = loader.validate_credential_schema(credentials)

        self.assertIsNone(result)
        self.assertEquals(['version: [\'required field\']'], errors)


class test_load_extraction(unittest.TestCase):
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
                            'target-column': 'Account Name',
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
