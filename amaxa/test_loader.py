import unittest
from unittest.mock import Mock
from . import amaxa, loader

class test_schemas(unittest.TestCase):
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
        self.assertEqual({}, errors)

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
        self.assertEqual({}, errors)

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
    
    def test_extraction_schema_validates_with_credentials(self):
        pass
    def test_validate_extraction_schema_returns_normalized_input(self):
        pass
    def test_validate_extraction_schema_returns_errors(self):
        pass
    def test_validate_credential_schema_returns_normalized_input(self):
        pass
    def test_validate_credential_schema_returns_errors(self):
        pass

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

    def test_validate_extraction_passes(self):
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
                }
            ]
        }

        m = unittest.mock.mock_open()
        with unittest.mock.patch('builtins.open', m):
            (result, errors) = loader.load_extraction(ex, context)
            
        m.assert_called_once_with('Account.csv', 'w')

        self.assertIsInstance(result, amaxa.MultiObjectExtraction)
        self.assertEqual([], errors)

    def test_load_extraction_finds_readable_field_group(self):
        pass
    def test_load_extraction_finds_writeable_field_group(self):
        pass
    def test_load_extraction_generates_field_list(self):
        pass
    def test_load_extraction_creates_export_mapper(self):
        pass
    def test_load_extraction_chooses_correct_scope(self):
        pass
    def test_load_extraction_opens_export_files(self):
        pass
    def test_load_extraction_creates_all_steps(self):
        pass