import unittest
from .. import amaxa, loader


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
