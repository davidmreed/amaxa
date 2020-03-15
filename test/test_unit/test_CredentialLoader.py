import unittest
from unittest.mock import Mock, patch

import simple_salesforce

from amaxa import loader


class test_CredentialLoader(unittest.TestCase):
    def _run_validation_test(self, input_data):
        with patch("simple_salesforce.Salesforce") as sf_mock:
            with patch("salesforce_bulk.SalesforceBulk"):
                sf_mock.return_value.bulk_url = "https://salesforce.com"

                credential_loader = loader.CredentialLoader(input_data)

                credential_loader.load()
                self.assertEqual([], credential_loader.errors)
                self.assertIsNotNone(credential_loader.result)

    def _run_failure_test(self, input_data, errors):
        with patch("simple_salesforce.Salesforce") as sf_mock:
            with patch("salesforce_bulk.SalesforceBulk"):
                sf_mock.return_value.bulk_url = "https://salesforce.com"

                credential_loader = loader.CredentialLoader(input_data)

                credential_loader.load()
                if type(errors) is list:
                    self.assertEqual(errors, credential_loader.errors)
                else:
                    self.assertEqual(errors, len(credential_loader.errors))

                self.assertIsNone(credential_loader.result)

    def _run_authentication_test(self, input_data, arguments):
        with patch("simple_salesforce.Salesforce") as sf_mock:
            with patch("salesforce_bulk.SalesforceBulk"):
                sf_mock.return_value.bulk_url = "https://salesforce.com"

                credential_loader = loader.CredentialLoader(input_data)

                credential_loader.load()
                self.assertEqual([], credential_loader.errors)
                self.assertIsNotNone(credential_loader.result)

                sf_mock.assert_called_once_with(version="46.0", **arguments)

    def test_credential_schema_validates_username_password_v1(self):
        self._run_validation_test(
            {
                "version": 1,
                "credentials": {
                    "password": "123456",
                    "username": "baltar@ucaprica.cc",
                    "security-token": "98765",
                    "sandbox": True,
                },
            }
        )

    def test_credential_schema_validates_access_token_v1(self):
        self._run_validation_test(
            {
                "version": 1,
                "credentials": {
                    "access-token": "ABCDEF123456",
                    "instance-url": "test.salesforce.com",
                },
            }
        )

    def test_credential_schema_validates_jwt_v1(self):
        with patch("amaxa.jwt_auth.jwt_login") as jwt_mock:
            jwt_mock.return_value.bulk_url = "https://salesforce.com"

            self._run_validation_test(
                {
                    "version": 1,
                    "credentials": {
                        "consumer-key": "ABCDEF123456",
                        "jwt-key": "--BEGIN KEY HERE--",
                        "username": "baltar@ucaprica.cc",
                    },
                }
            )

    def test_credential_schema_validates_jwt_key_file_v1(self):
        with patch("amaxa.jwt_auth.jwt_login") as jwt_mock:
            jwt_mock.return_value.bulk_url = "https://salesforce.com"

            m = unittest.mock.mock_open(read_data="00000")
            with patch("builtins.open", m):
                self._run_validation_test(
                    {
                        "version": 1,
                        "credentials": {
                            "consumer-key": "ABCDEF123456",
                            "jwt-file": "jwt.key",
                            "username": "baltar@ucaprica.cc",
                        },
                    }
                )

    def test_credential_schema_fails_mixed_values_v1(self):
        self._run_failure_test(
            {
                "version": 1,
                "credentials": {
                    "password": "123456",
                    "username": "baltar@ucaprica.cc",
                    "security-token": "98765",
                    "sandbox": True,
                    "instance-url": "test.salesforce.com",
                },
            },
            1,
        )

    def test_load_credentials_uses_username_password_v1(self):
        self._run_authentication_test(
            {
                "version": 1,
                "credentials": {
                    "password": "123456",
                    "username": "baltar@ucaprica.cc",
                    "security-token": "98765",
                    "sandbox": True,
                },
            },
            {
                "username": "baltar@ucaprica.cc",
                "password": "123456",
                "security_token": "98765",
                "organizationId": "",
                "sandbox": True,
            },
        )

    @patch("requests.post")
    @patch("jwt.encode")
    def test_load_credentials_uses_jwt_key_v1(self, jwt_mock, requests_mock):
        requests_mock.return_value.status_code = 200
        requests_mock.return_value.json = Mock(
            return_value={
                "instance_url": "test.salesforce.com",
                "access_token": "swordfish",
            }
        )
        self._run_authentication_test(
            {
                "version": 1,
                "credentials": {
                    "consumer-key": "123456",
                    "username": "baltar@ucaprica.cc",
                    "jwt-key": "00000",
                    "sandbox": True,
                },
            },
            {"session_id": "swordfish", "instance_url": "test.salesforce.com"},
        )

        jwt_mock.assert_called()
        requests_mock.assert_called_once_with(
            "https://test.salesforce.com/services/oauth2/token",
            data={
                "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
                "assertion": jwt_mock.return_value,
            },
        )

    @patch("requests.post")
    @patch("jwt.encode")
    def test_load_credentials_uses_jwt_file_v1(self, jwt_mock, requests_mock):
        requests_mock.return_value.status_code = 200
        requests_mock.return_value.json = Mock(
            return_value={
                "instance_url": "test.salesforce.com",
                "access_token": "swordfish",
            }
        )

        m = unittest.mock.mock_open(read_data="00000")
        with patch("builtins.open", m):
            self._run_authentication_test(
                {
                    "version": 1,
                    "credentials": {
                        "consumer-key": "123456",
                        "username": "baltar@ucaprica.cc",
                        "jwt-file": "jwt.key",
                        "sandbox": True,
                    },
                },
                {"session_id": "swordfish", "instance_url": "test.salesforce.com"},
            )

        m.assert_any_call("jwt.key", "r")
        jwt_mock.assert_called()
        requests_mock.assert_called_once_with(
            "https://test.salesforce.com/services/oauth2/token",
            data={
                "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
                "assertion": jwt_mock.return_value,
            },
        )

    @patch("requests.post")
    @patch("jwt.encode")
    def test_load_credentials_returns_error_on_jwt_key_failure_v1(
        self, jwt_mock, requests_mock
    ):
        body = {"error": "bad JWT", "error_description": "key error"}
        requests_mock.return_value.status_code = 400
        requests_mock.return_value.json = Mock(return_value=body)

        self._run_failure_test(
            {
                "version": 1,
                "credentials": {
                    "consumer-key": "123456",
                    "username": "baltar@ucaprica.cc",
                    "jwt-key": "00000",
                    "sandbox": True,
                },
            },
            ["Failed to authenticate with JWT: key error"],
        )

    @patch("requests.post")
    @patch("jwt.encode")
    def test_load_credentials_returns_error_on_jwt_file_failure_v1(
        self, jwt_mock, requests_mock
    ):
        body = {"error": "bad JWT", "error_description": "key error"}
        requests_mock.return_value.status_code = 400
        requests_mock.return_value.json = Mock(return_value=body)
        m = unittest.mock.mock_open(read_data="00000")
        with patch("builtins.open", m):
            self._run_failure_test(
                {
                    "version": 1,
                    "credentials": {
                        "consumer-key": "123456",
                        "username": "baltar@ucaprica.cc",
                        "jwt-file": "server.key",
                        "sandbox": True,
                    },
                },
                ["Failed to authenticate with JWT: key error"],
            )

    def test_load_credentials_uses_access_token_v1(self):
        self._run_authentication_test(
            {
                "version": 1,
                "credentials": {
                    "access-token": "ABCDEF123456",
                    "instance-url": "test.salesforce.com",
                },
            },
            {"session_id": "ABCDEF123456", "instance_url": "test.salesforce.com"},
        )

    def test_load_credentials_returns_validation_errors_v1(self):
        self._run_failure_test(
            {
                "credentials": {
                    "username": "baltar@ucaprica.edu",
                    "password": "666666666",
                }
            },
            ["No version number present in schema"],
        )

    def test_load_credentials_returns_error_without_credentials_v1(self):
        self._run_failure_test(
            {"version": 1, "credentials": {}},
            ["A set of valid credentials was not provided."],
        )

    def test_credential_schema_validates_username_password_v2(self):
        self._run_validation_test(
            {
                "version": 2,
                "credentials": {
                    "username": {
                        "password": "123456",
                        "username": "baltar@ucaprica.cc",
                        "security-token": "98765",
                    }
                },
            }
        )

    def test_credential_schema_validates_access_token_v2(self):
        self._run_validation_test(
            {
                "version": 2,
                "credentials": {
                    "token": {
                        "access-token": "ABCDEF123456",
                        "instance-url": "test.salesforce.com",
                    }
                },
            }
        )

    def test_credential_schema_validates_jwt_v2(self):
        with patch("amaxa.jwt_auth.jwt_login") as jwt_mock:
            jwt_mock.return_value.bulk_url = "https://salesforce.com"

            self._run_validation_test(
                {
                    "version": 2,
                    "credentials": {
                        "jwt": {
                            "consumer-key": "ABCDEF123456",
                            "key": "--BEGIN KEY HERE--",
                            "username": "baltar@ucaprica.cc",
                        }
                    },
                }
            )

    def test_credential_schema_validates_jwt_key_file_v2(self):
        with patch("amaxa.jwt_auth.jwt_login") as jwt_mock:
            jwt_mock.return_value.bulk_url = "https://salesforce.com"

            m = unittest.mock.mock_open(read_data="00000")
            with patch("builtins.open", m):
                self._run_validation_test(
                    {
                        "version": 2,
                        "credentials": {
                            "jwt": {
                                "consumer-key": "ABCDEF123456",
                                "keyfile": "jwt.key",
                                "username": "baltar@ucaprica.cc",
                            }
                        },
                    }
                )

    def test_load_credentials_uses_username_password_v2(self):
        self._run_authentication_test(
            {
                "version": 2,
                "credentials": {
                    "username": {
                        "password": "123456",
                        "username": "baltar@ucaprica.cc",
                        "security-token": "98765",
                    },
                    "sandbox": True,
                },
            },
            {
                "username": "baltar@ucaprica.cc",
                "password": "123456",
                "security_token": "98765",
                "organizationId": "",
                "sandbox": True,
            },
        )

    @patch("requests.post")
    @patch("jwt.encode")
    def test_load_credentials_uses_jwt_key_v2(self, jwt_mock, requests_mock):
        requests_mock.return_value.status_code = 200
        requests_mock.return_value.json = Mock(
            return_value={
                "instance_url": "test.salesforce.com",
                "access_token": "swordfish",
            }
        )
        self._run_authentication_test(
            {
                "version": 2,
                "credentials": {
                    "jwt": {
                        "consumer-key": "123456",
                        "username": "baltar@ucaprica.cc",
                        "key": "00000",
                    },
                    "sandbox": True,
                },
            },
            {"session_id": "swordfish", "instance_url": "test.salesforce.com"},
        )

        jwt_mock.assert_called()
        requests_mock.assert_called_once_with(
            "https://test.salesforce.com/services/oauth2/token",
            data={
                "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
                "assertion": jwt_mock.return_value,
            },
        )

    @patch("requests.post")
    @patch("jwt.encode")
    def test_load_credentials_uses_jwt_file_v2(self, jwt_mock, requests_mock):
        requests_mock.return_value.status_code = 200
        requests_mock.return_value.json = Mock(
            return_value={
                "instance_url": "test.salesforce.com",
                "access_token": "swordfish",
            }
        )

        m = unittest.mock.mock_open(read_data="00000")
        with patch("builtins.open", m):
            self._run_authentication_test(
                {
                    "version": 2,
                    "credentials": {
                        "jwt": {
                            "consumer-key": "123456",
                            "username": "baltar@ucaprica.cc",
                            "keyfile": "jwt.key",
                        },
                        "sandbox": True,
                    },
                },
                {"session_id": "swordfish", "instance_url": "test.salesforce.com"},
            )

        m.assert_any_call("jwt.key", "r")
        jwt_mock.assert_called()
        requests_mock.assert_called_once_with(
            "https://test.salesforce.com/services/oauth2/token",
            data={
                "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
                "assertion": jwt_mock.return_value,
            },
        )

    @patch("requests.post")
    @patch("jwt.encode")
    def test_load_credentials_returns_error_on_jwt_key_failure_v2(
        self, jwt_mock, requests_mock
    ):
        body = {"error": "bad JWT", "error_description": "key error"}
        requests_mock.return_value.status_code = 400
        requests_mock.return_value.json = Mock(return_value=body)

        self._run_failure_test(
            {
                "version": 2,
                "credentials": {
                    "jwt": {
                        "consumer-key": "123456",
                        "username": "baltar@ucaprica.cc",
                        "key": "00000",
                    },
                    "sandbox": True,
                },
            },
            ["Failed to authenticate with JWT: key error"],
        )

    @patch("requests.post")
    @patch("jwt.encode")
    def test_load_credentials_returns_error_on_jwt_file_failure_v2(
        self, jwt_mock, requests_mock
    ):
        body = {"error": "bad JWT", "error_description": "key error"}
        requests_mock.return_value.status_code = 400
        requests_mock.return_value.json = Mock(return_value=body)
        m = unittest.mock.mock_open(read_data="00000")
        with patch("builtins.open", m):
            self._run_failure_test(
                {
                    "version": 2,
                    "credentials": {
                        "jwt": {
                            "consumer-key": "123456",
                            "username": "baltar@ucaprica.cc",
                            "keyfile": "server.key",
                        },
                        "sandbox": True,
                    },
                },
                ["Failed to authenticate with JWT: key error"],
            )

    def test_load_credentials_uses_access_token_v2(self):
        self._run_authentication_test(
            {
                "version": 2,
                "credentials": {
                    "token": {
                        "access-token": "ABCDEF123456",
                        "instance-url": "test.salesforce.com",
                    }
                },
            },
            {"session_id": "ABCDEF123456", "instance_url": "test.salesforce.com"},
        )

    @patch("os.environ")
    def test_load_credentials_uses_environment_variable(self, environ_mock):
        environ_mock.get = Mock(return_value="ABCDEF123456")

        self._run_authentication_test(
            {
                "version": 2,
                "credentials": {
                    "token": {
                        "access-token": {"env": "ACCESS_TOKEN"},
                        "instance-url": "test.salesforce.com",
                    }
                },
            },
            {"session_id": "ABCDEF123456", "instance_url": "test.salesforce.com"},
        )

    def test_load_credentials_traps_login_errors(self):
        credentials = loader.CredentialLoader({})

        side_effect = simple_salesforce.SalesforceError(
            "https://salesforce.com", 401, "describe", ""
        )
        credentials.result = Mock()
        credentials.result.get_global_describe = Mock(side_effect=side_effect)

        credentials._post_validate()

        self.assertEqual(
            ["Unable to authenticate to Salesforce: {}".format(side_effect)],
            credentials.errors,
        )
        self.assertIsNone(credentials.result)
