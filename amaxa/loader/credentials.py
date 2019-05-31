import logging
import simple_salesforce
from .core import Loader, InputType
from .. import jwt_auth

class CredentialLoader(Loader):
    def __init__(self, in_dict):
        super().__init__(in_dict, InputType.CREDENTIALS)

    def _load(self):
        credentials = self.input['credentials']

        # Determine what type of credentials we have
        if 'username' in credentials and 'password' in credentials:
            # User + password, optional Security Token
            self.result = simple_salesforce.Salesforce(
                username=credentials['username'],
                password=credentials['password'],
                security_token=credentials.get('security-token', ''),
                organizationId=credentials.get('organization-id', ''),
                sandbox=credentials.get('sandbox', False)
            )

            logging.getLogger('amaxa').debug(
                'Authenticating to Salesforce with user name and password')
        elif 'username' in credentials and 'consumer-key' and 'jwt-key' in credentials:
            # JWT authentication with key provided inline.
            try:
                self.result = jwt_auth.jwt_login(
                    credentials['consumer-key'],
                    credentials['username'],
                    credentials['jwt-key'],
                    credentials.get('sandbox', False)
                )
                logging.getLogger('amaxa').debug('Authenticating to Salesforce with inline JWT key')
            except simple_salesforce.exceptions.SalesforceAuthenticationFailed as ex:
                self.errors.append('Failed to authenticate with JWT: {}'.format(ex.message))
        elif 'username' in credentials and 'jwt-file' in credentials:
            # JWT authentication with external keyfile.
            try:
                with open(credentials['jwt-file'], 'r') as jwt_file:
                    self.result = jwt_auth.jwt_login(
                        credentials['consumer-key'],
                        credentials['username'],
                        jwt_file.read(),
                        credentials.get('sandbox', False)
                    )
                logging.getLogger('amaxa').debug(
                    'Authenticating to Salesforce with external JWT key')
            except simple_salesforce.exceptions.SalesforceAuthenticationFailed as ex:
                self.errors.append('Failed to authenticate with JWT: {}'.format(ex.message))
        elif 'access-token' in credentials and 'instance-url' in credentials:
            self.result = simple_salesforce.Salesforce(
                instance_url=credentials['instance-url'],
                session_id=credentials['access-token']
            )
            logging.getLogger('amaxa').debug('Authenticating to Salesforce with access token')
        else:
            self.errors.append('A set of valid credentials was not provided.')

    def _post_validate(self):
        try:
            self.result.describe()
        except Exception as e:
            self.errors.append('Unable to authenticate to Salesforce: {}'.format(e))
            self.result = None
