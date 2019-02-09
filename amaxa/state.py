import yaml
import json
import cerberus
from . import amaxa

def save_state(operation, json_mode = False):
    output = {
        'version': 1,
        'state': {
            'stage': operation.stage.value,
            'id-map': { str(k): str(v) for k, v in operation.global_id_map.items() }
        }
    }

    return yaml.dump(output) if not json_mode else json.dumps(output)

def load_state(operation, state_data, json_mode = False):
    (state, errors) = validate_state_schema(yaml.safe_load(state_data) if not json_mode else json.load(state_data))

    if len(errors) == 0:
        operation.stage = amaxa.LoadStage.values_dict()[state['state']['stage']]
        operation.global_id_map = { amaxa.SalesforceId(k): amaxa.SalesforceId(v) for k, v in state['state']['id-map'].items() }

        return (operation, [])
    
    return (None, errors)

def validate_state_schema(input):
    v = cerberus.Validator(state_schema)
    return (
        v.validated(input),
        ['{}: {}'.format(k, v.errors[k]) for k in v.errors]
    )


state_schema = {
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