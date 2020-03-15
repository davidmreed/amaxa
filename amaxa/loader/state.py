import json

import yaml

from .. import amaxa
from .core import Loader
from .input_type import InputType


def save_state(operation, json_mode=False):
    output = {
        "version": 1,
        "state": {
            "stage": operation.stage.value,
            "id-map": {str(k): str(v) for k, v in operation.global_id_map.items()},
        },
    }

    return yaml.dump(output) if not json_mode else json.dumps(output)


class StateLoader(Loader):
    def __init__(self, in_dict, operation):
        super().__init__(in_dict, InputType.STATE)
        self.result = operation

    def _load(self):
        self.result.stage = amaxa.LoadStage.values_dict()[self.input["state"]["stage"]]
        self.result.global_id_map = {
            amaxa.SalesforceId(k): amaxa.SalesforceId(v)
            for k, v in self.input["state"]["id-map"].items()
        }
