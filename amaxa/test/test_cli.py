import unittest
import json
import yaml
import io
from unittest.mock import Mock
from .. import loader, amaxa
from ..__main__ import main as main


CREDENTIALS_GOOD_YAML = '''
version: 1
credentials:
    username: 'test@example.com'
    password: 'blah'
    security-token: '00000'
    sandbox: True
'''

CREDENTIALS_GOOD_JSON = '''
{
    "version": 1,
    "credentials": {
        "username": "test@example.com",
        "password": "blah",
        "security-token": "00000",
        "sandbox": true
    }
}
'''

CREDENTIALS_BAD = '''
credentials:
    username: 'test@example.com'
    password: 'blah'
    security-token: '00000'
    sandbox: True
'''

EXTRACTION_GOOD_YAML = '''
version: 1
operation:
    - 
        sobject: Account
        fields: 
            - Name
            - Id
            - ParentId
        extract: 
            all: True
'''
EXTRACTION_GOOD_JSON = '''
{
    "version": 1,
    "extraction": [
        {
            "sobject": "Account",
            "fields": [
                "Name",
                "Id",
                "ParentId"
            ],
            "extract": {
                "all": true
            }
        }
    ]
}
'''
EXTRACTION_BAD = '''
operation:
    - 
        sobject: Account
        fields: 
            - Name
            - Id
            - ParentId
        extract: 
            all: True
'''

STATE_GOOD_YAML = '''
version: 1
state:
    stage: inserts
    id-map:
        '001000000000001': '001000000000002'
        '001000000000003': '001000000000004'
'''

state_file = io.StringIO()

def select_file(f, *args, **kwargs):
    data = { 
        'credentials-bad.yaml': CREDENTIALS_BAD,
        'extraction-bad.yaml': EXTRACTION_BAD,
        'extraction-good.yaml': EXTRACTION_GOOD_YAML,
        'credentials-good.yaml': CREDENTIALS_GOOD_YAML,
        'credentials-good.json': CREDENTIALS_GOOD_JSON,
        'extraction-good.json': EXTRACTION_GOOD_JSON,
        'state-good.yaml': STATE_GOOD_YAML,
        'extraction-good.state.yaml': state_file
    }
    if type(data[f]) is str:
        m = unittest.mock.mock_open(read_data=data[f])(f, *args, **kwargs)
        m.name = f
    else:
        m = data[f]
    
    return m

class test_CLI(unittest.TestCase):
    @unittest.mock.patch('amaxa.__main__.CredentialLoader')
    @unittest.mock.patch('amaxa.__main__.ExtractionOperationLoader')
    def test_main_calls_execute_with_json_input_extract_mode(self, operation_mock, credential_mock):
        context = Mock()
        context.run.return_value = 0
        credential_mock.return_value = Mock()
        credential_mock.return_value.result = context
        credential_mock.return_value.errors = []
        operation_mock.return_value = Mock()
        operation_mock.return_value.result = context
        operation_mock.return_value.errors = []
        
        m = Mock(side_effect=select_file)
        with unittest.mock.patch('builtins.open', m):
            with unittest.mock.patch(
                'sys.argv',
                ['amaxa', '-c', 'credentials-good.json', 'extraction-good.json']
            ):
                return_value = main()

        credential_mock.assert_called_once_with(json.loads(CREDENTIALS_GOOD_JSON))
        operation_mock.assert_called_once_with(json.loads(EXTRACTION_GOOD_JSON), context)

        context.run.assert_called_once_with()

        self.assertEqual(0, return_value)
    
    @unittest.mock.patch('amaxa.__main__.CredentialLoader')
    @unittest.mock.patch('amaxa.__main__.LoadOperationLoader')
    def test_main_calls_execute_with_json_input_load_mode(self, operation_mock, credential_mock):
        context = Mock()
        context.run.return_value = 0
        credential_mock.return_value = Mock()
        credential_mock.return_value.result = context
        credential_mock.return_value.errors = []
        operation_mock.return_value = Mock()
        operation_mock.return_value.result = context
        operation_mock.return_value.errors = []
        
        m = Mock(side_effect=select_file)
        with unittest.mock.patch('builtins.open', m):
            with unittest.mock.patch(
                'sys.argv',
                ['amaxa', '-c', 'credentials-good.json', '--load', 'extraction-good.json']
            ):
                return_value = main()

        credential_mock.assert_called_once_with(json.loads(CREDENTIALS_GOOD_JSON))
        operation_mock.assert_called_once_with(json.loads(EXTRACTION_GOOD_JSON), context, use_state=False)

        context.run.assert_called_once_with()

        self.assertEqual(0, return_value)

    @unittest.mock.patch('amaxa.__main__.CredentialLoader')
    @unittest.mock.patch('amaxa.__main__.ExtractionOperationLoader')
    def test_main_calls_execute_with_yaml_input(self, operation_mock, credential_mock):
        context = Mock()
        context.run.return_value = 0
        credential_mock.return_value = Mock()
        credential_mock.return_value.result = context
        credential_mock.return_value.errors = []
        operation_mock.return_value = Mock()
        operation_mock.return_value.result = context
        operation_mock.return_value.errors = []

        m = Mock(side_effect=select_file)
        with unittest.mock.patch('builtins.open', m):
            with unittest.mock.patch(
                'sys.argv',
                ['amaxa', '-c', 'credentials-good.yaml', 'extraction-good.yaml']
            ):
                return_value = main()

        credential_mock.assert_called_once_with(yaml.safe_load(CREDENTIALS_GOOD_YAML))
        operation_mock.assert_called_once_with(yaml.safe_load(EXTRACTION_GOOD_YAML), context)

        context.run.assert_called_once_with()

        self.assertEqual(0, return_value)

    @unittest.mock.patch('amaxa.__main__.CredentialLoader')
    @unittest.mock.patch('amaxa.__main__.ExtractionOperationLoader')
    def test_main_returns_error_with_bad_credentials(self, operation_mock, credential_mock):
        context = Mock()
        credential_mock.return_value = Mock()
        credential_mock.return_value.result = None
        credential_mock.return_value.errors = ['Test error occured.']
        operation_mock.return_value = Mock()
        operation_mock.return_value.result = context
        operation_mock.return_value.errors = []
        
        m = Mock(side_effect=select_file)
        with unittest.mock.patch('builtins.open', m):
            with unittest.mock.patch(
                'sys.argv',
                ['amaxa', '-c', 'credentials-bad.yaml', 'extraction-good.yaml']
            ):
                return_value = main()

        credential_mock.assert_called_once_with(yaml.safe_load(CREDENTIALS_BAD))
        context.run.assert_not_called()

        self.assertEqual(-1, return_value)

    @unittest.mock.patch('amaxa.__main__.CredentialLoader')
    @unittest.mock.patch('amaxa.__main__.ExtractionOperationLoader')
    def test_main_returns_error_with_bad_extraction(self, operation_mock, credential_mock):
        context = Mock()
        credential_mock.return_value = Mock()
        credential_mock.return_value.result = context
        credential_mock.return_value.errors = []
        operation_mock.return_value = Mock()
        operation_mock.return_value.result = None
        operation_mock.return_value.errors = ['Test error occured.']

        m = Mock(side_effect=select_file)
        with unittest.mock.patch('builtins.open', m):
            with unittest.mock.patch(
                'sys.argv',
                ['amaxa', '-c', 'credentials-good.yaml', 'extraction-bad.yaml']
            ):
                return_value = main()

        credential_mock.assert_called_once_with(yaml.safe_load(CREDENTIALS_GOOD_YAML))
        operation_mock.assert_called_once_with(yaml.safe_load(EXTRACTION_BAD), context)

        self.assertEqual(-1, return_value)
    
    @unittest.mock.patch('amaxa.__main__.CredentialLoader')
    @unittest.mock.patch('amaxa.__main__.ExtractionOperationLoader')
    def test_main_returns_error_with_errors_during_extraction(self, operation_mock, credential_mock):
        context = Mock()
        op = Mock()
        op.run = Mock(return_value=-1)
        op.stage = amaxa.LoadStage.INSERTS
        op.global_id_map = {}

        credential_mock.return_value = Mock()
        credential_mock.return_value.result = context
        credential_mock.return_value.errors = []
        operation_mock.return_value = Mock()
        operation_mock.return_value.result = op
        operation_mock.return_value.errors = []

        m = Mock(side_effect=select_file)
        with unittest.mock.patch('builtins.open', m):
            with unittest.mock.patch(
                'sys.argv',
                ['amaxa', '-c', 'credentials-good.yaml', 'extraction-good.yaml']
            ):
                return_value = main()

        self.assertEqual(-1, return_value)

    @unittest.mock.patch('amaxa.__main__.CredentialLoader')
    @unittest.mock.patch('amaxa.__main__.ExtractionOperationLoader')
    def test_main_saves_state_on_error(self, operation_mock, credential_mock):
        context = Mock()
        op = Mock()
        op.run = Mock(return_value=-1)
        op.stage = amaxa.LoadStage.INSERTS
        op.global_id_map = { amaxa.SalesforceId('001000000000001'): amaxa.SalesforceId('001000000000002') }

        credential_mock.return_value = Mock()
        credential_mock.return_value.result = context
        credential_mock.return_value.errors = []
        operation_mock.return_value = Mock()
        operation_mock.return_value.result = op
        operation_mock.return_value.errors = []

        m = Mock(side_effect=select_file)
        with unittest.mock.patch('builtins.open', m):
            with unittest.mock.patch(
                'sys.argv',
                ['amaxa', '-c', 'credentials-good.yaml', 'extraction-good.yaml']
            ):
                return_value = main()

        self.assertEqual(-1, return_value)
        self.assertLess(0, state_file.tell())

        state_file.seek(0)
        yaml_state = yaml.load(state_file)

        self.assertIn('state', yaml_state)
        self.assertIn('id-map', yaml_state['state'])
        self.assertIn('stage', yaml_state['state'])
        self.assertEqual(amaxa.LoadStage.INSERTS.value, yaml_state['state']['stage'])
        self.assertEqual(
            { str(k) : str(v) for k, v in op.global_id_map.items() },
            yaml_state['state']['id-map']
        )

    @unittest.mock.patch('amaxa.__main__.CredentialLoader')
    @unittest.mock.patch('amaxa.__main__.ExtractionOperationLoader')
    def test_main_loads_state_with_use_state_option(self, operation_mock, credential_mock):
        context = Mock()
        op = Mock()
        op.run = Mock(return_value=0)

        credential_mock.return_value = Mock()
        credential_mock.return_value.result = context
        credential_mock.return_value.errors = []
        operation_mock.return_value = Mock()
        operation_mock.return_value.result = op
        operation_mock.return_value.errors = []

        m = Mock(side_effect=select_file)
        with unittest.mock.patch('builtins.open', m):
            with unittest.mock.patch(
                'sys.argv',
                ['amaxa', '-c', 'credentials-good.yaml', 'extraction-good.yaml', '--use-state', 'state-good.yaml']
            ):
                return_value = main()

        self.assertEqual(0, return_value)
        self.assertEqual(amaxa.LoadStage.INSERTS, op.stage)
        self.assertEqual(
            {
                amaxa.SalesforceId('001000000000001'): amaxa.SalesforceId('001000000000002'),
                amaxa.SalesforceId('001000000000003'): amaxa.SalesforceId('001000000000004')
            },
            op.global_id_map
        )
