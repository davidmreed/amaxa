import pytest
import unittest
from unittest.mock import Mock

from salesforce_bulk import UploadResult

import amaxa
from amaxa import constants

from .MockConnection import MockConnection
from .MockFileStore import MockFileStore


class test_LoadStep(unittest.TestCase):
    def test_stores_lookup_behaviors(self):
        load_step = amaxa.LoadStep("Account", ["Name", "ParentId"])

        self.assertEqual(
            amaxa.OutsideLookupBehavior.INCLUDE,
            load_step.get_lookup_behavior_for_field("ParentId"),
        )

        load_step.set_lookup_behavior_for_field(
            "ParentId", amaxa.OutsideLookupBehavior.ERROR
        )
        self.assertEqual(
            amaxa.OutsideLookupBehavior.ERROR,
            load_step.get_lookup_behavior_for_field("ParentId"),
        )

    def test_get_value_for_lookup_with_parent_available(self):
        connection = Mock()
        op = amaxa.LoadOperation(connection)
        op.file_store = Mock()
        op.register_new_id(
            "Account",
            amaxa.SalesforceId("001000000000000"),
            amaxa.SalesforceId("001000000000001"),
        )

        load_step = amaxa.LoadStep("Account", ["Name", "ParentId"])
        op.add_step(load_step)

        self.assertEqual(
            load_step.get_value_for_lookup(
                "ParentId", "001000000000000", "001000000000002"
            ),
            str(amaxa.SalesforceId("001000000000001")),
        )

    def test_get_value_for_lookup_with_blank_input(self):
        connection = Mock()
        op = amaxa.LoadOperation(connection)

        load_step = amaxa.LoadStep("Account", ["Name", "ParentId"])
        op.add_step(load_step)

        self.assertEqual(
            load_step.get_value_for_lookup("ParentId", "", "001000000000002"), ""
        )

    def test_get_value_for_lookup_with_include_behavior(self):
        connection = Mock()
        op = amaxa.LoadOperation(connection)

        load_step = amaxa.LoadStep("Account", ["Name", "ParentId"])
        op.add_step(load_step)

        self.assertEqual(
            load_step.get_value_for_lookup(
                "ParentId", "001000000000000", "001000000000002"
            ),
            "001000000000000",
        )

    def test_get_value_for_lookup_with_drop_behavior(self):
        connection = Mock()
        op = amaxa.LoadOperation(connection)

        load_step = amaxa.LoadStep("Account", ["Name", "ParentId"])
        op.add_step(load_step)

        load_step.set_lookup_behavior_for_field(
            "ParentId", amaxa.OutsideLookupBehavior.DROP_FIELD
        )

        self.assertEqual(
            load_step.get_value_for_lookup(
                "ParentId", "001000000000000", "001000000000002"
            ),
            "",
        )

    def test_get_value_for_lookup_with_error_behavior(self):
        connection = Mock()
        op = amaxa.LoadOperation(connection)

        load_step = amaxa.LoadStep("Account", ["Name", "ParentId"])
        op.add_step(load_step)

        load_step.set_lookup_behavior_for_field(
            "ParentId", amaxa.OutsideLookupBehavior.ERROR
        )

        with self.assertRaises(
            amaxa.AmaxaException,
            msg="{} {} has an outside reference in field {} ({}), "
            "which is not allowed by the extraction configuration.".format(
                "Account", "001000000000002", "ParentId", "001000000000000"
            ),
        ):
            load_step.get_value_for_lookup(
                "ParentId", "001000000000000", "001000000000002"
            )

    def test_populates_lookups(self):
        load_step = amaxa.LoadStep("Account", ["Name", "ParentId"])
        load_step.get_value_for_lookup = Mock(return_value="001000000000002")

        record = {
            "Id": "001000000000000",
            "Name": "Test",
            "ParentId": "001000000000001",
        }

        self.assertEqual(
            {"Id": "001000000000000", "Name": "Test", "ParentId": "001000000000002"},
            load_step.populate_lookups(record, ["ParentId"], "001000000000000"),
        )

    def test_converts_data_for_bulk_api(self):
        connection = Mock()
        op = amaxa.LoadOperation(connection)
        op.get_field_map = Mock(
            return_value={
                "Name": {"soapType": "xsd:string"},
                "Boolean__c": {"soapType": "xsd:boolean"},
                "Boolean_False__c": {"soapType": "xsd:boolean"},
                "Id": {"soapType": "tns:ID"},
                "Date__c": {"soapType": "xsd:date"},
                "DateTime__c": {"soapType": "xsd:dateTime"},
                "Int__c": {"soapType": "xsd:int"},
                "Double__c": {"soapType": "xsd:double"},
                "Random__c": {"soapType": "xsd:string"},
            }
        )

        load_step = amaxa.LoadStep("Account", ["Name", "ParentId"])
        op.add_step(load_step)

        record = {
            "Name": "Test",
            "Boolean__c": "yes",
            "Boolean_False__c": "no",
            "Id": "001000000000001",
            "Date__c": "2018-12-31",
            "DateTime__c": "2018-12-31T00:00:00.000Z",
            "Int__c": "100",
            "Double__c": "10.1",
            "Random__c": "",
        }

        self.assertEqual(
            {
                "Name": "Test",
                "Boolean__c": "true",
                "Boolean_False__c": "false",
                "Id": "001000000000001",
                "Date__c": "2018-12-31",
                "DateTime__c": "2018-12-31T00:00:00.000Z",
                "Int__c": "100",
                "Double__c": "10.1",
                "Random__c": None,
            },
            load_step.primitivize(record),
        )

    def test_converts_data_for_bulk_api__failures(self):
        connection = Mock()
        op = amaxa.LoadOperation(connection)
        op.get_field_map = Mock(
            return_value={
                "Boolean__c": {"soapType": "xsd:boolean"},
                "Address__c": {"soapType": "xsd:address"},
            }
        )

        load_step = amaxa.LoadStep("Account", ["Name", "ParentId"])
        op.add_step(load_step)

        with pytest.raises(ValueError):
            load_step.primitivize({"Boolean__c": "blah"})

        assert load_step.primitivize({"Address__c": "foo"})["Address__c"] is None

    def test_transform_records_calls_context_mapper(self):
        connection = Mock()
        op = amaxa.LoadOperation(connection)
        op.mappers["Account"] = Mock()
        op.mappers["Account"].transform_record = Mock(
            return_value={"Name": "Test2", "ParentId": "001000000000001"}
        )

        load_step = amaxa.LoadStep("Account", ["Name", "ParentId"])
        op.add_step(load_step)
        load_step.dependent_lookups = set()
        load_step.self_lookups = set()

        self.assertEqual(
            {"Name": "Test2", "ParentId": "001000000000001"},
            load_step.transform_record(
                {"Name": "Test1", "ParentId": "001000000000000"}
            ),
        )
        op.mappers["Account"].transform_record.assert_called_once_with(
            {"Name": "Test1", "ParentId": "001000000000000"}
        )

    def test_transform_records_cleans_excess_fields(self):
        connection = Mock()
        op = amaxa.LoadOperation(connection)

        load_step = amaxa.LoadStep("Account", ["Name", "ParentId"])
        op.add_step(load_step)
        load_step.dependent_lookups = set()
        load_step.self_lookups = set()

        self.assertEqual(
            {"Name": "Test2", "ParentId": "001000000000001"},
            load_step.transform_record(
                {"Name": "Test2", "ParentId": "001000000000001", "Excess__c": True}
            ),
        )

    def test_transform_records_runs_transform_before_cleaning(self):
        connection = Mock()
        op = amaxa.LoadOperation(connection)
        op.mappers["Account"] = Mock()
        op.mappers["Account"].transform_record = Mock(
            return_value={
                "Name": "Test2",
                "ParentId": "001000000000001",
                "Excess__c": True,
            }
        )

        load_step = amaxa.LoadStep("Account", ["Name", "ParentId"])
        op.add_step(load_step)
        load_step.dependent_lookups = set()
        load_step.self_lookups = set()

        self.assertEqual(
            {"Name": "Test2", "ParentId": "001000000000001"},
            load_step.transform_record(
                {
                    "Account Name": "Test2",
                    "ParentId": "001000000000001",
                    "Excess__c": True,
                }
            ),
        )

    def test_extract_dependent_lookups_returns_dependent_fields(self):
        load_step = amaxa.LoadStep("Account", ["Id", "Name", "ParentId"])
        load_step.self_lookups = set(["ParentId"])
        load_step.dependent_lookups = set()

        self.assertEqual(
            {"Id": "001000000000001", "ParentId": "001000000000002"},
            load_step.extract_dependent_lookups(
                {
                    "Name": "Gemenon Gastronomics",
                    "Id": "001000000000001",
                    "ParentId": "001000000000002",
                }
            ),
        )

    def test_clean_dependent_lookups_returns_clean_record(self):
        load_step = amaxa.LoadStep("Account", ["Id", "Name", "ParentId"])
        load_step.self_lookups = set(["ParentId"])
        load_step.dependent_lookups = set()

        self.assertEqual(
            {"Name": "Gemenon Gastronomics", "Id": "001000000000001"},
            load_step.clean_dependent_lookups(
                {
                    "Name": "Gemenon Gastronomics",
                    "Id": "001000000000001",
                    "ParentId": "001000000000002",
                }
            ),
        )

    def test_execute_transforms_and_loads_records_without_lookups(self):
        record_list = [
            {"Name": "Test", "Id": "001000000000000"},
            {"Name": "Test 2", "Id": "001000000000001"},
        ]
        clean_record_list = [{"Name": "Test"}, {"Name": "Test 2"}]
        connection = MockConnection(
            bulk_insert_results=[
                UploadResult("001000000000002", True, True, ""),
                UploadResult("001000000000003", True, True, ""),
            ]
        )
        op = amaxa.LoadOperation(Mock(wraps=connection))
        op.file_store = MockFileStore()
        op.register_new_id = Mock()
        op.file_store.records["Account"] = record_list
        op.mappers["Account"] = Mock()
        op.mappers["Account"].transform_record = Mock(side_effect=lambda x: x)

        load_step = amaxa.LoadStep("Account", ["Name"])
        op.add_step(load_step)
        load_step.primitivize = Mock(side_effect=lambda x: x)
        load_step.populate_lookups = Mock(side_effect=lambda x, y, z: x)

        load_step.initialize()
        load_step.execute()

        op.mappers["Account"].transform_record.assert_has_calls(
            [unittest.mock.call(x) for x in record_list]
        )
        load_step.primitivize.assert_has_calls(
            [unittest.mock.call(x) for x in clean_record_list]
        )
        load_step.populate_lookups.assert_has_calls(
            [
                unittest.mock.call(x, set(), y["Id"])
                for (x, y) in zip(clean_record_list, record_list)
            ]
        )

        op.connection.bulk_api_insert.assert_called_once_with(
            "Account",
            clean_record_list,
            load_step.get_option("bulk-api-timeout"),
            load_step.get_option("bulk-api-poll-interval"),
            load_step.get_option("bulk-api-batch-size"),
        )
        op.register_new_id.assert_has_calls(
            [
                unittest.mock.call(
                    "Account",
                    amaxa.SalesforceId("001000000000000"),
                    amaxa.SalesforceId("001000000000002"),
                ),
                unittest.mock.call(
                    "Account",
                    amaxa.SalesforceId("001000000000001"),
                    amaxa.SalesforceId("001000000000003"),
                ),
            ]
        )

    def test_execute_transforms_and_loads_records_with_lookups(self):
        record_list = [
            {"Name": "Test", "Id": "001000000000000", "OwnerId": "500000000000000"},
            {"Name": "Test 2", "Id": "001000000000001", "OwnerId": "500000000000001"},
        ]
        transformed_record_list = [
            {"Name": "Test", "OwnerId": str(amaxa.SalesforceId("500000000000002"))},
            {"Name": "Test 2", "OwnerId": str(amaxa.SalesforceId("500000000000003"))},
        ]

        connection = MockConnection(
            bulk_insert_results=[
                UploadResult("001000000000002", True, True, ""),
                UploadResult("001000000000003", True, True, ""),
            ]
        )
        op = amaxa.LoadOperation(Mock(wraps=connection))
        op.file_store = MockFileStore()

        op.register_new_id(
            "User",
            amaxa.SalesforceId("500000000000000"),
            amaxa.SalesforceId("500000000000002"),
        )
        op.register_new_id(
            "User",
            amaxa.SalesforceId("500000000000001"),
            amaxa.SalesforceId("500000000000003"),
        )

        op.register_new_id = Mock()
        op.file_store.records["Account"] = record_list
        op.mappers["Account"] = Mock()
        op.mappers["Account"].transform_record = Mock(side_effect=lambda x: x)

        load_step = amaxa.LoadStep("Account", ["Name", "OwnerId"])
        op.add_step(load_step)
        load_step.primitivize = Mock(side_effect=lambda x: x)

        load_step.initialize()
        load_step.descendent_lookups = set(["OwnerId"])

        load_step.execute()

        op.mappers["Account"].transform_record.assert_has_calls(
            [unittest.mock.call(x) for x in record_list]
        )
        load_step.primitivize.assert_has_calls(
            [unittest.mock.call(x) for x in transformed_record_list]
        )

        op.connection.bulk_api_insert.assert_called_once_with(
            "Account",
            transformed_record_list,
            load_step.get_option("bulk-api-timeout"),
            load_step.get_option("bulk-api-poll-interval"),
            load_step.get_option("bulk-api-batch-size"),
        )
        op.register_new_id.assert_has_calls(
            [
                unittest.mock.call(
                    "Account",
                    amaxa.SalesforceId("001000000000000"),
                    amaxa.SalesforceId("001000000000002"),
                ),
                unittest.mock.call(
                    "Account",
                    amaxa.SalesforceId("001000000000001"),
                    amaxa.SalesforceId("001000000000003"),
                ),
            ]
        )

    def test_execute_loads_high_volume_records(self):
        connection = MockConnection(
            bulk_insert_results=[
                UploadResult("001000000{:06d}".format(i + 1), True, True, "")
                for i in range(20000)
            ]
        )
        op = amaxa.LoadOperation(Mock(wraps=connection))
        op.file_store = MockFileStore()
        op.register_new_id = Mock()

        record_list = [
            {"Id": "001000000{:06d}".format(i), "Name": "Account {:06d}".format(i)}
            for i in range(20000)
        ]
        op.file_store.records["Account"] = record_list
        op.get_result_file = Mock()

        load_step = amaxa.LoadStep("Account", ["Name"])
        op.add_step(load_step)

        load_step.initialize()
        load_step.execute()

        self.assertEqual(20000, op.register_new_id.call_count)

        # Validate that the correct Ids were mapped
        # Each Id should be mapped to itself plus one.
        for index, each_call in enumerate(op.register_new_id.call_args_list):
            self.assertEqual(
                unittest.mock.call(
                    "Account",
                    amaxa.SalesforceId("001000000{:06d}".format(index)),
                    amaxa.SalesforceId("001000000{:06d}".format(index + 1)),
                ),
                each_call,
            )

    def test_execute_handles_errors(self):
        record_list = [
            {"Name": "Test", "Id": "001000000000000"},
            {"Name": "Test 2", "Id": "001000000000001"},
        ]
        error = [
            {
                "statusCode": "DUPLICATES_DETECTED",
                "message": "There are duplicates",
                "fields": [],
                "extendedErrorDetails": None,
            }
        ]
        connection = MockConnection(
            bulk_insert_results=[
                UploadResult(None, False, False, error),
                UploadResult(None, False, False, error),
            ]
        )
        op = amaxa.LoadOperation(Mock(wraps=connection))
        op.file_store = MockFileStore()
        op.file_store.records["Account"] = record_list
        op.register_new_id = Mock()
        op.register_error = Mock()

        load_step = amaxa.LoadStep("Account", ["Name"])
        op.add_step(load_step)

        load_step.initialize()
        load_step.execute()

        self.assertEqual(
            [
                unittest.mock.call(
                    "Account", record_list[0]["Id"], load_step.format_error(error)
                ),
                unittest.mock.call(
                    "Account", record_list[1]["Id"], load_step.format_error(error)
                ),
            ],
            op.register_error.call_args_list,
        )

    def test_execute_handles_exceptions__outside_lookups(self):
        record_list = [
            {"Name": "Test", "Id": "001000000000000", "ParentId": ""},
            {"Name": "Test 2", "Id": "001000000000001", "ParentId": "001000000000002"},
        ]
        connection = MockConnection()
        op = amaxa.LoadOperation(Mock(wraps=connection))
        op.file_store = MockFileStore()
        op.file_store.records["Account"] = record_list
        op.register_new_id = Mock()
        op.register_error = Mock()

        load_step = amaxa.LoadStep("Account", ["Name", "ParentId"])
        load_step.set_lookup_behavior_for_field(
            "ParentId", amaxa.OutsideLookupBehavior.ERROR
        )
        op.add_step(load_step)

        load_step.initialize()

        # Force a failure by treating ParentId as a descendent lookup
        load_step.descendent_lookups = {"ParentId"}
        load_step.self_lookups = set()
        load_step.dependent_lookups = set()

        load_step.execute()

        self.assertEqual(
            [
                unittest.mock.call(
                    "Account",
                    record_list[1]["Id"],
                    f"Account {record_list[1]['Id']} has an outside reference in field ParentId (001000000000002), which is not allowed by the extraction configuration.",
                ),
            ],
            op.register_error.call_args_list,
        )

    def test_execute_handles_exceptions__bad_data(self):
        record_list = [
            {"Name": "Test", "Id": "001000000000000", "IsDeleted": "false"},
            {"Name": "Test 2", "Id": "001000000000001", "IsDeleted": "foo"},
        ]
        connection = MockConnection()
        op = amaxa.LoadOperation(Mock(wraps=connection))
        op.file_store = MockFileStore()
        op.file_store.records["Account"] = record_list
        op.register_new_id = Mock()
        op.register_error = Mock()

        load_step = amaxa.LoadStep("Account", ["Name", "IsDeleted"])
        op.add_step(load_step)

        load_step.initialize()
        load_step.execute()

        self.assertEqual(
            [
                unittest.mock.call(
                    "Account",
                    record_list[1]["Id"],
                    f"Bad data in record {record_list[1]['Id']}: Invalid Boolean value foo",
                ),
            ],
            op.register_error.call_args_list,
        )

    def test_execute_dependent_updates_handles_lookups(self):
        record_list = [
            {"Name": "Test", "Id": "001000000000000", "ParentId": "001000000000004"},
            {"Name": "Test 2", "Id": "001000000000001", "ParentId": "001000000000005"},
        ]
        transformed_record_list = [
            {
                "Id": str(amaxa.SalesforceId("001000000000002")),
                "ParentId": str(amaxa.SalesforceId("001000000000006")),
            },
            {
                "Id": str(amaxa.SalesforceId("001000000000003")),
                "ParentId": str(amaxa.SalesforceId("001000000000007")),
            },
        ]

        connection = MockConnection(
            bulk_update_results=[
                UploadResult("001000000000002", True, True, ""),
                UploadResult("001000000000003", True, True, ""),
            ]
        )
        op = amaxa.LoadOperation(Mock(wraps=connection))
        op.file_store = MockFileStore()
        op.register_new_id(
            "Account",
            amaxa.SalesforceId("001000000000004"),
            amaxa.SalesforceId("001000000000006"),
        )
        op.register_new_id(
            "Account",
            amaxa.SalesforceId("001000000000005"),
            amaxa.SalesforceId("001000000000007"),
        )
        op.register_new_id(
            "Account",
            amaxa.SalesforceId("001000000000000"),
            amaxa.SalesforceId("001000000000002"),
        )
        op.register_new_id(
            "Account",
            amaxa.SalesforceId("001000000000001"),
            amaxa.SalesforceId("001000000000003"),
        )
        op.register_new_id = Mock()
        op.register_error = Mock()
        op.file_store.records["Account"] = record_list

        load_step = amaxa.LoadStep("Account", ["Name", "ParentId"])
        op.add_step(load_step)

        load_step.initialize()
        load_step.execute_dependent_updates()

        op.register_error.assert_not_called()
        op.connection.bulk_api_update.assert_called_once_with(
            "Account",
            transformed_record_list,
            load_step.get_option("bulk-api-timeout"),
            load_step.get_option("bulk-api-poll-interval"),
            load_step.get_option("bulk-api-batch-size"),
        )

    def test_execute_dependent_updates_handles_errors(self):
        record_list = [
            {"Name": "Test", "Id": "001000000000000", "ParentId": "001000000000001"},
            {"Name": "Test 2", "Id": "001000000000001", "ParentId": "001000000000000"},
        ]
        error = [
            {
                "statusCode": "DUPLICATES_DETECTED",
                "message": "There are duplicates",
                "fields": [],
                "extendedErrorDetails": None,
            }
        ]
        connection = MockConnection(
            bulk_update_results=[
                UploadResult(None, False, False, error),
                UploadResult(None, False, False, error),
            ]
        )
        op = amaxa.LoadOperation(Mock(wraps=connection))
        op.file_store = MockFileStore()

        op.register_new_id(
            "Account",
            amaxa.SalesforceId("001000000000000"),
            amaxa.SalesforceId("001000000000002"),
        )
        op.register_new_id(
            "Account",
            amaxa.SalesforceId("001000000000001"),
            amaxa.SalesforceId("001000000000003"),
        )

        op.register_new_id = Mock()
        op.register_error = Mock()
        op.file_store.records["Account"] = record_list

        load_step = amaxa.LoadStep("Account", ["Name", "ParentId"])
        op.add_step(load_step)

        load_step.initialize()
        load_step.execute_dependent_updates()

        self.assertEqual(
            [
                unittest.mock.call(
                    "Account", record_list[0]["Id"], load_step.format_error(error)
                ),
                unittest.mock.call(
                    "Account", record_list[1]["Id"], load_step.format_error(error)
                ),
            ],
            op.register_error.call_args_list,
        )

    def test_execute_does_not_insert_records_prepopulated_in_id_map(self):
        record_list = [
            {"Name": "Test", "Id": "001000000000000"},
            {"Name": "Test 2", "Id": "001000000000001"},
            {"Name": "Test 3", "Id": "001000000000002"},
        ]
        clean_record_list = [{"Name": "Test 2"}, {"Name": "Test 3"}]
        connection = MockConnection(
            bulk_insert_results=[
                UploadResult("001000000000007", True, True, ""),
                UploadResult("001000000000008", True, True, ""),
            ]
        )
        op = amaxa.LoadOperation(Mock(wraps=connection))
        op.file_store = MockFileStore()
        op.register_new_id(
            "Account",
            amaxa.SalesforceId("001000000000000"),
            amaxa.SalesforceId("001000000000005"),
        )
        op.register_new_id = Mock()
        op.file_store.records["Account"] = record_list
        op.mappers["Account"] = Mock()
        op.mappers["Account"].transform_record = Mock(side_effect=lambda x: x)

        load_step = amaxa.LoadStep("Account", ["Name"])
        op.add_step(load_step)

        load_step.primitivize = Mock(side_effect=lambda x: x)
        load_step.populate_lookups = Mock(side_effect=lambda x, y, z: x)

        load_step.initialize()
        load_step.execute()

        op.connection.bulk_api_insert.assert_called_once_with(
            "Account",
            clean_record_list,
            load_step.get_option("bulk-api-timeout"),
            load_step.get_option("bulk-api-poll-interval"),
            load_step.get_option("bulk-api-batch-size"),
        )
        op.register_new_id.assert_has_calls(
            [
                unittest.mock.call(
                    "Account",
                    amaxa.SalesforceId("001000000000001"),
                    amaxa.SalesforceId("001000000000007"),
                ),
                unittest.mock.call(
                    "Account",
                    amaxa.SalesforceId("001000000000002"),
                    amaxa.SalesforceId("001000000000008"),
                ),
            ]
        )

    def test_execute_does_not_run_bulk_job_if_all_records_inserted(self):
        record_list = [{"Name": "Test", "Id": "001000000000000"}]
        connection = MockConnection()
        op = amaxa.LoadOperation(Mock(wraps=connection))
        op.file_store = MockFileStore()
        op.register_new_id(
            "Account",
            amaxa.SalesforceId("001000000000000"),
            amaxa.SalesforceId("001000000000005"),
        )
        op.register_new_id = Mock()
        op.file_store.records["Account"] = record_list

        load_step = amaxa.LoadStep("Account", ["Name"])
        op.add_step(load_step)

        load_step.initialize()
        load_step.execute()

        op.connection.bulk_api_insert.assert_not_called()

    def test_format_error_constructs_messages(self):
        step = amaxa.LoadStep("Account", ["Name"])

        self.assertEqual(
            "DUPLICATES_DETECTED: There are duplicates\nOTHER_ERROR: "
            "There are non-duplicates (Name, Id). More info",
            step.format_error(
                [
                    {
                        "statusCode": "DUPLICATES_DETECTED",
                        "message": "There are duplicates",
                        "fields": [],
                        "extendedErrorDetails": None,
                    },
                    {
                        "statusCode": "OTHER_ERROR",
                        "message": "There are non-duplicates",
                        "fields": ["Name", "Id"],
                        "extendedErrorDetails": "More info",
                    },
                ]
            ),
        )

    def test_get_option_set(self):
        step = amaxa.LoadStep("Account", ["Name"], options={"bulk-api-batch-size": 1})

        self.assertEqual(1, step.get_option("bulk-api-batch-size"))

    def test_get_option_default(self):
        step = amaxa.LoadStep("Account", ["Name"], options={})

        self.assertEqual(
            constants.OPTION_DEFAULTS["bulk-api-batch-size"],
            step.get_option("bulk-api-batch-size"),
        )

    def test_execute_uses_custom_bulk_api_options(self):
        record_list = [
            {"Name": "Test", "Id": "001000000000000"},
            {"Name": "Test 2", "Id": "001000000000001"},
        ]
        cleaned_record_list = [{"Name": "Test"}, {"Name": "Test 2"}]
        connection = MockConnection(
            bulk_insert_results=[
                UploadResult("001000000000002", True, True, ""),
                UploadResult("001000000000003", True, True, ""),
            ]
        )
        op = amaxa.LoadOperation(Mock(wraps=connection))
        op.file_store = MockFileStore()
        op.register_new_id = Mock()
        op.file_store.records["Account"] = record_list

        step = amaxa.LoadStep(
            "Account",
            ["Name"],
            options={
                "bulk-api-poll-interval": 10,
                "bulk-api-timeout": 600,
                "bulk-api-batch-size": 5000,
            },
        )
        step.context = op
        step.primitivize = Mock(side_effect=lambda x: x)
        step.populate_lookups = Mock(side_effect=lambda x, y, z: x)

        step.initialize()
        step.execute()

        op.connection.bulk_api_insert.assert_called_once_with(
            "Account", cleaned_record_list, 600, 10, 5000
        )

    def test_execute_dependent_updates_uses_bulk_api_options(self):
        record_list = [
            {"Name": "Test", "Id": "001000000000000", "ParentId": "001000000000001"},
            {"Name": "Test 2", "Id": "001000000000001", "ParentId": "001000000000000"},
        ]
        cleaned_record_list = [
            {"Id": "001000000000002AAA", "ParentId": "001000000000003AAA"},
            {"Id": "001000000000003AAA", "ParentId": "001000000000002AAA"},
        ]

        connection = MockConnection(
            bulk_update_results=[
                UploadResult("001000000000002", True, True, ""),
                UploadResult("001000000000003", True, True, ""),
            ]
        )
        op = amaxa.LoadOperation(Mock(wraps=connection))
        op.file_store = MockFileStore()

        op.register_new_id(
            "Account",
            amaxa.SalesforceId("001000000000000"),
            amaxa.SalesforceId("001000000000002"),
        )
        op.register_new_id(
            "Account",
            amaxa.SalesforceId("001000000000001"),
            amaxa.SalesforceId("001000000000003"),
        )

        op.register_new_id = Mock()
        op.register_error = Mock()
        op.file_store.records["Account"] = record_list

        load_step = amaxa.LoadStep(
            "Account",
            ["Name", "ParentId"],
            options={
                "bulk-api-poll-interval": 10,
                "bulk-api-timeout": 600,
                "bulk-api-batch-size": 5000,
            },
        )
        op.add_step(load_step)

        load_step.initialize()
        load_step.execute_dependent_updates()

        op.connection.bulk_api_update.assert_called_once_with(
            "Account", cleaned_record_list, 600, 10, 5000
        )
