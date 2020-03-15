import unittest
from unittest.mock import Mock

import amaxa


class test_Step(unittest.TestCase):
    def test_initialize_identifies_self_lookups(self):
        connection = Mock()

        oc = amaxa.ExtractOperation(connection)

        oc.file_store.set_csv("Account", amaxa.FileType.OUTPUT, Mock())
        oc.get_field_map = Mock(
            return_value={
                "Lookup__c": {
                    "name": "Lookup__c",
                    "type": "reference",
                    "referenceTo": ["Account"],
                },
                "Other__c": {
                    "name": "Other__c",
                    "type": "reference",
                    "referenceTo": ["Contact"],
                },
            }
        )

        step = amaxa.Step("Account", ["Lookup__c", "Other__c"])
        oc.add_step(step)

        step.initialize()

        self.assertEqual(set(["Lookup__c"]), step.self_lookups)

    def test_initialize_identifies_dependent_lookups(self):
        connection = Mock()

        oc = amaxa.ExtractOperation(connection)

        oc.file_store.set_csv("Account", amaxa.FileType.OUTPUT, Mock())
        oc.file_store.set_csv("Contact", amaxa.FileType.OUTPUT, Mock())
        oc.get_field_map = Mock(
            return_value={
                "Lookup__c": {
                    "name": "Lookup__c",
                    "type": "reference",
                    "referenceTo": ["Account"],
                },
                "Other__c": {
                    "name": "Other__c",
                    "type": "reference",
                    "referenceTo": ["Contact"],
                },
            }
        )
        oc.get_sobject_list = Mock(return_value=["Account", "Contact"])

        step = amaxa.Step("Account", ["Lookup__c", "Other__c"])
        oc.add_step(step)

        step.initialize()

        self.assertEqual(set(["Other__c"]), step.dependent_lookups)

    def test_initialize_identifies_all_lookups_within_extraction(self):
        connection = Mock()

        oc = amaxa.ExtractOperation(connection)

        oc.file_store.set_csv("Account", amaxa.FileType.OUTPUT, Mock())
        oc.file_store.set_csv("Contact", amaxa.FileType.OUTPUT, Mock())
        oc.get_field_map = Mock(
            return_value={
                "Lookup__c": {
                    "name": "Lookup__c",
                    "type": "reference",
                    "referenceTo": ["Account"],
                },
                "Other__c": {
                    "name": "Other__c",
                    "type": "reference",
                    "referenceTo": ["Contact"],
                },
                "Outside__c": {
                    "name": "Outside__c",
                    "type": "reference",
                    "referenceTo": ["Opportunity"],
                },
            }
        )
        oc.get_sobject_list = Mock(return_value=["Account", "Contact"])

        step = amaxa.Step("Account", ["Lookup__c", "Other__c", "Outside__c"])
        oc.add_step(step)

        step.initialize()

        self.assertEqual(set(["Other__c", "Lookup__c"]), step.all_lookups)

    def test_initialize_identifies_descendent_lookups(self):
        connection = Mock()

        oc = amaxa.ExtractOperation(connection)

        oc.file_store.set_csv("Account", amaxa.FileType.OUTPUT, Mock())
        oc.file_store.set_csv("Contact", amaxa.FileType.OUTPUT, Mock())
        oc.get_field_map = Mock(
            return_value={
                "Lookup__c": {
                    "name": "Lookup__c",
                    "type": "reference",
                    "referenceTo": ["Account"],
                },
                "Other__c": {
                    "name": "Other__c",
                    "type": "reference",
                    "referenceTo": ["Contact"],
                },
            }
        )
        oc.get_sobject_list = Mock(return_value=["Account", "Contact"])

        step = amaxa.Step("Contact", ["Lookup__c", "Other__c"])
        oc.add_step(step)

        step.initialize()

        self.assertEqual(set(["Lookup__c"]), step.descendent_lookups)

    def test_initialize_handles_mixed_polymorphic_lookups(self):
        connection = Mock()

        oc = amaxa.ExtractOperation(connection)

        oc.file_store.set_csv("Account", amaxa.FileType.OUTPUT, Mock())
        oc.file_store.set_csv("Contact", amaxa.FileType.OUTPUT, Mock())
        oc.file_store.set_csv("Opportunity", amaxa.FileType.OUTPUT, Mock())
        oc.get_field_map = Mock(
            return_value={
                "Poly_Lookup__c": {
                    "name": "Lookup__c",
                    "type": "reference",
                    "referenceTo": ["Account", "Opportunity"],
                },
                "Other__c": {
                    "name": "Other__c",
                    "type": "reference",
                    "referenceTo": ["Contact"],
                },
            }
        )
        oc.get_sobject_list = Mock(return_value=["Account", "Contact", "Opportunity"])

        step = amaxa.Step("Contact", ["Poly_Lookup__c", "Other__c"])
        oc.add_step(step)

        step.initialize()

        self.assertEqual(set(["Poly_Lookup__c"]), step.dependent_lookups)
        self.assertEqual(set(["Poly_Lookup__c"]), step.descendent_lookups)

    def test_generates_field_list(self):
        step = amaxa.Step("Account", ["Lookup__c", "Other__c"])

        self.assertEqual("Lookup__c, Other__c", step.get_field_list())
