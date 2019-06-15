import json
import copy
import os.path
from simple_salesforce import Salesforce
from unittest.mock import MagicMock, Mock

sobject_describes = {}
sobject_list = ["Account", "Contact", "Opportunity", "Task", "Attachment"]

with open(os.path.join("assets", "test_describes", "sobjects.json"), "r") as d:
    root_describe = json.load(d)

for sobject in sobject_list:
    with open(os.path.join("assets", "test_describes", f"{sobject}.json"), "r") as d:
        sobject_describes[sobject] = json.load(d)


class MockSimpleSalesforce(object):
    def __init__(self):
        self._describe = None
        self._sobject_describes = {}
        for sobject in sobject_list:
            setattr(
                self,
                sobject,
                Mock(describe=Mock(side_effect=lambda s=sobject: self.get_describe(s))),
            )

    def describe(self):
        if self._describe is None:
            self._describe = copy.deepcopy(root_describe)

        return self._describe

    def get_describe(self, sobject):
        if sobject not in self._sobject_describes:
            self._sobject_describes[sobject] = copy.deepcopy(sobject_describes[sobject])

        return self._sobject_describes[sobject]
