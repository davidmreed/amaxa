import json
import unittest
from unittest.mock import Mock

import yaml

import amaxa
from amaxa.loader import StateLoader, save_state

EXAMPLE_DICT = {
    "version": 1,
    "state": {
        "stage": "inserts",
        "id-map": {
            "001000000000000AAA": "001000000000001AAA",
            "001000000000002AAA": "001000000000003AAA",
        },
    },
}

EXAMPLE_ID_MAP = {
    amaxa.SalesforceId(k): amaxa.SalesforceId(v)
    for k, v in EXAMPLE_DICT["state"]["id-map"].items()
}


class test_StateLoader(unittest.TestCase):
    def test_loads_state(self):
        sl = StateLoader(EXAMPLE_DICT, Mock())

        sl.load()

        self.assertEqual(EXAMPLE_ID_MAP, sl.result.global_id_map)
        self.assertEqual(amaxa.LoadStage.INSERTS, sl.result.stage)

    def test_saves_state_json(self):
        operation = Mock()
        operation.global_id_map = EXAMPLE_ID_MAP
        operation.stage = amaxa.LoadStage.INSERTS

        self.assertEqual(EXAMPLE_DICT, yaml.safe_load(save_state(operation)))

    def test_saves_state_yaml(self):
        operation = Mock()
        operation.global_id_map = EXAMPLE_ID_MAP
        operation.stage = amaxa.LoadStage.INSERTS

        self.assertEqual(
            EXAMPLE_DICT, json.loads(save_state(operation, json_mode=True))
        )
