import logging

import simple_salesforce

from .. import api, constants, jwt_auth
from .core import Loader
from .input_type import InputType


class CredentialLoader(Loader):
    def __init__(self, in_dict):
        super().__init__(in_dict, InputType.CREDENTIALS)

    def _load(self):
        if self.input["version"] == 1:
            self._load_v1()
        else:
            self._load_v2()

    def _load_v1(self):
        credentials = self.input["credentials"]

        # Determine what type of credentials we have
        if "username" in credentials and "password" in credentials:
            # User + password, optional Security Token
            self.result = simple_salesforce.Salesforce(
                username=credentials["username"],
                password=credentials["password"],
                security_token=credentials.get("security-token", ""),
                organizationId=credentials.get("organization-id", ""),
                sandbox=credentials.get("sandbox", False),
                version=constants.API_VERSION,
            )

            logging.getLogger("amaxa").debug(
                "Authenticating to Salesforce with user name and password"
            )
        elif "username" in credentials and "consumer-key" and "jwt-key" in credentials:
            # JWT authentication with key provided inline.
            try:
                self.result = jwt_auth.jwt_login(
                    credentials["consumer-key"],
                    credentials["username"],
                    credentials["jwt-key"],
                    credentials.get("sandbox", False),
                )
                logging.getLogger("amaxa").debug(
                    "Authenticating to Salesforce with inline JWT key"
                )
            except simple_salesforce.exceptions.SalesforceAuthenticationFailed as ex:
                self.errors.append(
                    "Failed to authenticate with JWT: {}".format(ex.message)
                )
        elif "username" in credentials and "jwt-file" in credentials:
            # JWT authentication with external keyfile.
            try:
                with open(credentials["jwt-file"], "r") as jwt_file:
                    self.result = jwt_auth.jwt_login(
                        credentials["consumer-key"],
                        credentials["username"],
                        jwt_file.read(),
                        credentials.get("sandbox", False),
                    )
                logging.getLogger("amaxa").debug(
                    "Authenticating to Salesforce with external JWT key"
                )
            except simple_salesforce.exceptions.SalesforceAuthenticationFailed as ex:
                self.errors.append(
                    "Failed to authenticate with JWT: {}".format(ex.message)
                )
        elif "access-token" in credentials and "instance-url" in credentials:
            self.result = simple_salesforce.Salesforce(
                instance_url=credentials["instance-url"],
                session_id=credentials["access-token"],
                version=constants.API_VERSION,
            )
            logging.getLogger("amaxa").debug(
                "Authenticating to Salesforce with access token"
            )
        else:
            self.errors.append("A set of valid credentials was not provided.")

        if self.result is not None:
            self.result = api.Connection(self.result)

    def _load_v2(self):
        credentials = self.input["credentials"]

        # Determine what type of credentials we have
        sandbox = credentials["sandbox"]
        if "username" in credentials:
            # User + password, optional Security Token
            credentials = credentials["username"]
            self.result = simple_salesforce.Salesforce(
                username=credentials["username"],
                password=credentials["password"],
                security_token=credentials.get("security-token", ""),
                organizationId=credentials.get("organization-id", ""),
                sandbox=sandbox,
                version=constants.API_VERSION,
            )

            logging.getLogger("amaxa").debug(
                "Authenticating to Salesforce with user name and password"
            )
        elif "jwt" in credentials:
            credentials = credentials["jwt"]
            if "keyfile" in credentials:
                # JWT authentication with external keyfile
                with open(credentials["keyfile"], "r") as jwt_file:
                    key = jwt_file.read()

                logging.getLogger("amaxa").debug(
                    "Authenticating to Salesforce with external JWT key"
                )
            else:
                # JWT authentication with key provided inline.
                key = credentials["key"]

            try:
                self.result = jwt_auth.jwt_login(
                    credentials["consumer-key"], credentials["username"], key, sandbox
                )
            except simple_salesforce.exceptions.SalesforceAuthenticationFailed as ex:
                self.errors.append(
                    "Failed to authenticate with JWT: {}".format(ex.message)
                )
        elif "token" in credentials:
            credentials = credentials["token"]
            self.result = simple_salesforce.Salesforce(
                instance_url=credentials["instance-url"],
                session_id=credentials["access-token"],
                version=constants.API_VERSION,
            )
            logging.getLogger("amaxa").debug(
                "Authenticating to Salesforce with access token"
            )

        if self.result is not None:
            self.result = api.Connection(self.result)

    def _post_validate(self):
        try:
            self.result.get_global_describe()
        except simple_salesforce.SalesforceError as e:
            self.errors.append("Unable to authenticate to Salesforce: {}".format(e))
            self.result = None
