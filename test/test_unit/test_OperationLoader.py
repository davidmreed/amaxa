import unittest
from unittest.mock import Mock

import amaxa
from amaxa.loader import core
from amaxa.loader.input_type import InputType

from .MockConnection import MockConnection


class test_OperationLoader(unittest.TestCase):
    def test_validate_field_mapping_catches_duplicate_columns(self):
        ex = {
            "version": 1,
            "operation": [
                {
                    "sobject": "Account",
                    "fields": [
                        {"field": "Name", "column": "Industry"},
                        "Industry",
                        {"field": "Description", "column": "Industry"},
                    ],
                    "extract": {"all": True},
                }
            ],
        }

        context = core.OperationLoader(ex, None, InputType.EXTRACT_OPERATION)
        context._validate_field_mapping()

        self.assertEqual(
            ["Account: One or more columns is specified multiple times: Industry"],
            context.errors,
        )

    def test_validate_field_mapping_catches_duplicate_fields(self):
        ex = {
            "version": 1,
            "operation": [
                {
                    "sobject": "Account",
                    "fields": [
                        {"field": "Name", "column": "Industry"},
                        {"field": "Name", "column": "Name"},
                    ],
                    "extract": {"all": True},
                }
            ],
        }
        context = core.OperationLoader(ex, None, InputType.EXTRACT_OPERATION)
        context._validate_field_mapping()

        self.assertEqual(
            ["Account: One or more fields is specified multiple times: Name"],
            context.errors,
        )

    def test_get_data_mapper_creates_mapper_load(self):
        ex = {
            "sobject": "Account",
            "fields": [
                {
                    "field": "Name",
                    "column": "Account Name",
                    "transforms": ["strip", "lowercase"],
                },
                "Industry",
            ],
            "extract": {"all": True},
            "input-validation": "none",
        }

        context = core.OperationLoader({}, None, InputType.EXTRACT_OPERATION)
        mapper = context._get_data_mapper(ex, "column", "field")

        self.assertEqual({"Account Name": "Name"}, mapper.field_name_mapping)
        self.assertEqual(
            {"Name": "university of caprica", "Industry": "Education"},
            mapper.transform_record(
                {"Account Name": "UNIversity of caprica  ", "Industry": "Education"}
            ),
        )

    def test_get_data_mapper_creates_mapper_extract(self):
        ex = {
            "sobject": "Account",
            "fields": [
                {
                    "field": "Name",
                    "column": "Account Name",
                    "transforms": ["strip", "lowercase"],
                },
                "Industry",
            ],
            "extract": {"all": True},
            "input-validation": "none",
        }

        context = core.OperationLoader({}, None, InputType.LOAD_OPERATION)
        mapper = context._get_data_mapper(ex, "field", "column")

        self.assertEqual({"Name": "Account Name"}, mapper.field_name_mapping)
        self.assertEqual(
            {"Account Name": "university of caprica", "Industry": "Education"},
            mapper.transform_record(
                {"Name": "UNIversity of caprica  ", "Industry": "Education"}
            ),
        )

    def test_get_data_mapper_creates_mapper_transform_only(self):
        ex = {
            "sobject": "Account",
            "fields": [
                {"field": "Name", "transforms": ["strip", "lowercase"]},
                "Industry",
            ],
            "extract": {"all": True},
            "input-validation": "none",
        }

        context = core.OperationLoader({}, None, InputType.LOAD_OPERATION)
        mapper = context._get_data_mapper(ex, "field", "column")

        self.assertEqual({}, mapper.field_name_mapping)
        self.assertEqual(
            {"Name": "university of caprica", "Industry": "Education"},
            mapper.transform_record(
                {"Name": "UNIversity of caprica  ", "Industry": "Education"}
            ),
        )

    def test_get_data_mapper_creates_mapper_column_only(self):
        ex = {
            "sobject": "Account",
            "fields": [{"field": "Name", "column": "Account Name"}, "Industry"],
            "extract": {"all": True},
            "input-validation": "none",
        }

        context = core.OperationLoader({}, None, InputType.LOAD_OPERATION)
        mapper = context._get_data_mapper(ex, "field", "column")

        self.assertEqual({"Name": "Account Name"}, mapper.field_name_mapping)
        self.assertEqual(
            {"Account Name": "UNIversity of caprica", "Industry": "Education"},
            mapper.transform_record(
                {"Name": "UNIversity of caprica", "Industry": "Education"}
            ),
        )

    def test_validate_sobjects_flags_missing_sobjects(self):
        context = Mock()
        context.steps = []
        context.connection = MockConnection()

        ex = {
            "version": 1,
            "operation": [
                {"sobject": "Object__c", "fields": ["Name"], "extract": {"all": True}}
            ],
        }

        context = core.OperationLoader(ex, MockConnection(), InputType.LOAD_OPERATION)
        context._validate_sobjects("createable")

        self.assertEqual(
            [
                "sObject Object__c does not exist or does not have the correct permission (createable)"
            ],
            context.errors,
        )

    def test_validate_field_permissions_flags_fields(self):
        context = core.OperationLoader({}, None, InputType.LOAD_OPERATION)
        context.result = amaxa.Operation(MockConnection())
        context.result.steps = [Mock()]
        context.result.steps[0].sobjectname = "Account"
        context.result.steps[0].field_scope = set(["Name", "IsDeleted"])
        context._validate_field_permissions("createable")

        self.assertEqual(
            [
                "Field Account.IsDeleted does not exist or does not have the correct CRUD permission (createable)."
            ],
            context.errors,
        )
