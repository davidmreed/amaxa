import unittest

import amaxa
from amaxa import transforms


class test_DataMapper(unittest.TestCase):
    def test_transform_key_applies_mapping(self):
        mapper = amaxa.DataMapper({"Test": "Value"})

        self.assertEqual("Value", mapper.transform_key("Test"))
        self.assertEqual("Foo", mapper.transform_key("Foo"))

    def test_transform_value_applies_transformations(self):
        mapper = amaxa.DataMapper(
            {}, {"Test__c": [transforms.strip, transforms.lowercase]}
        )

        self.assertEqual("value", mapper.transform_value("Test__c", " VALUE  "))

    def test_transform_record_does(self):
        mapper = amaxa.DataMapper(
            {"Test__c": "Value"}, {"Test__c": [transforms.strip, transforms.lowercase]}
        )

        self.assertEqual(
            {"Value": "nothing much", "Second Key": "another Response"},
            mapper.transform_record(
                {"Test__c": "  NOTHING MUCH", "Second Key": "another Response"}
            ),
        )
