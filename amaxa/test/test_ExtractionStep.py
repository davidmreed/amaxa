import unittest
import json
from unittest.mock import Mock, PropertyMock, patch
from salesforce_bulk.util import IteratorBytesIO
from .. import amaxa


class test_ExtractionStep(unittest.TestCase):
    def test_retains_lookup_behavior_for_fields(self):
        step = amaxa.ExtractionStep(
            "Account",
            amaxa.ExtractionScope.ALL_RECORDS,
            ["Self_Lookup__c", "Other__c"],
            "",
            amaxa.SelfLookupBehavior.TRACE_NONE,
            amaxa.OutsideLookupBehavior.INCLUDE,
        )

        self.assertEqual(
            amaxa.SelfLookupBehavior.TRACE_NONE,
            step.get_self_lookup_behavior_for_field("Self_Lookup__c"),
        )
        step.set_lookup_behavior_for_field(
            "Self_Lookup__c", amaxa.SelfLookupBehavior.TRACE_ALL
        )
        self.assertEqual(
            amaxa.SelfLookupBehavior.TRACE_ALL,
            step.get_self_lookup_behavior_for_field("Self_Lookup__c"),
        )

        self.assertEqual(
            amaxa.OutsideLookupBehavior.INCLUDE,
            step.get_outside_lookup_behavior_for_field("Other__c"),
        )
        step.set_lookup_behavior_for_field(
            "Other__c", amaxa.OutsideLookupBehavior.DROP_FIELD
        )
        self.assertEqual(
            amaxa.OutsideLookupBehavior.DROP_FIELD,
            step.get_outside_lookup_behavior_for_field("Other__c"),
        )

    def test_store_result_calls_context(self):
        connection = Mock()

        oc = amaxa.ExtractOperation(connection)

        oc.store_result = Mock()
        oc.add_dependency = Mock()
        oc.get_field_map = Mock(
            return_value={
                "Lookup__c": {
                    "name": "Lookup__c",
                    "type": "reference",
                    "referenceTo": ["Account"],
                }
            }
        )
        oc.get_sobject_list = Mock(return_value=["Account"])

        step = amaxa.ExtractionStep("Account", amaxa.ExtractionScope.ALL_RECORDS, [])
        oc.add_step(step)
        step.initialize()

        step.store_result({"Id": "001000000000000", "Name": "Picon Fleet Headquarters"})
        oc.store_result.assert_called_once_with(
            "Account", {"Id": "001000000000000", "Name": "Picon Fleet Headquarters"}
        )
        oc.add_dependency.assert_not_called()

    def test_store_result_registers_self_lookup_dependencies(self):
        connection = Mock()

        oc = amaxa.ExtractOperation(connection)

        oc.store_result = Mock()
        oc.add_dependency = Mock()
        oc.get_field_map = Mock(
            return_value={
                "Lookup__c": {
                    "name": "Lookup__c",
                    "type": "reference",
                    "referenceTo": ["Account"],
                }
            }
        )
        oc.get_sobject_list = Mock(return_value=["Account"])

        step = amaxa.ExtractionStep(
            "Account", amaxa.ExtractionScope.ALL_RECORDS, ["Lookup__c"]
        )
        oc.add_step(step)
        step.initialize()

        step.store_result(
            {
                "Id": "001000000000000",
                "Lookup__c": "001000000000001",
                "Name": "Picon Fleet Headquarters",
            }
        )
        oc.add_dependency.assert_called_once_with(
            "Account", amaxa.SalesforceId("001000000000001")
        )

    def test_store_result_handles_empty_self_lookup_fields(self):
        connection = Mock()

        oc = amaxa.ExtractOperation(connection)

        oc.store_result = Mock()
        oc.add_dependency = Mock()
        oc.get_field_map = Mock(
            return_value={
                "Lookup__c": {
                    "name": "Lookup__c",
                    "type": "reference",
                    "referenceTo": ["Account"],
                }
            }
        )
        oc.get_sobject_list = Mock(return_value=["Account"])

        step = amaxa.ExtractionStep(
            "Account", amaxa.ExtractionScope.ALL_RECORDS, ["Lookup__c"]
        )
        oc.add_step(step)
        step.initialize()

        step.store_result(
            {
                "Id": "001000000000000",
                "Lookup__c": None,
                "Name": "Picon Fleet Headquarters",
            }
        )
        oc.add_dependency.assert_not_called()

    def test_store_result_respects_self_lookup_options(self):
        connection = Mock()

        oc = amaxa.ExtractOperation(connection)

        oc.store_result = Mock()
        oc.add_dependency = Mock()
        oc.get_field_map = Mock(
            return_value={
                "Lookup__c": {
                    "name": "Lookup__c",
                    "type": "reference",
                    "referenceTo": ["Account"],
                }
            }
        )
        oc.get_sobject_list = Mock(return_value=["Account"])

        step = amaxa.ExtractionStep(
            "Account",
            amaxa.ExtractionScope.ALL_RECORDS,
            ["Lookup__c"],
            None,
            amaxa.SelfLookupBehavior.TRACE_NONE,
        )
        oc.add_step(step)
        step.initialize()

        step.store_result(
            {
                "Id": "001000000000000",
                "Lookup__c": "001000000000001",
                "Name": "Picon Fleet Headquarters",
            }
        )
        oc.add_dependency.assert_not_called()

    def test_store_result_registers_dependent_lookup_dependencies(self):
        connection = Mock()

        oc = amaxa.ExtractOperation(connection)

        oc.store_result = Mock()
        oc.add_dependency = Mock()
        oc.get_field_map = Mock(
            return_value={
                "Lookup__c": {
                    "name": "Lookup__c",
                    "type": "reference",
                    "referenceTo": ["Opportunity"],
                }
            }
        )
        oc.get_sobject_list = Mock(return_value=["Account", "Opportunity"])

        step = amaxa.ExtractionStep(
            "Account", amaxa.ExtractionScope.ALL_RECORDS, ["Lookup__c"]
        )
        oc.add_step(step)
        step.initialize()

        step.store_result(
            {
                "Id": "001000000000000",
                "Lookup__c": "006000000000001",
                "Name": "Picon Fleet Headquarters",
            }
        )
        oc.add_dependency.assert_called_once_with(
            "Opportunity", amaxa.SalesforceId("006000000000001")
        )

    def test_store_result_handles_polymorphic_lookups(self):
        connection = Mock()

        oc = amaxa.ExtractOperation(connection)

        oc.store_result = Mock()
        oc.add_dependency = Mock()
        oc.get_field_map = Mock(
            return_value={
                "Lookup__c": {
                    "name": "Lookup__c",
                    "type": "reference",
                    "referenceTo": ["Opportunity", "Account", "Task"],
                }
            }
        )
        oc.get_sobject_list = Mock(return_value=["Account", "Contact", "Opportunity"])
        oc.get_extracted_ids = Mock(return_value=["001000000000001"])
        oc.get_sobject_name_for_id = Mock(
            side_effect=lambda id: {
                "001": "Account",
                "006": "Opportunity",
                "00T": "Task",
            }[id[:3]]
        )

        step = amaxa.ExtractionStep(
            "Contact", amaxa.ExtractionScope.ALL_RECORDS, ["Lookup__c"]
        )
        oc.add_step(step)
        step.initialize()

        # Validate that the polymorphic lookup is treated properly when the content is a dependent reference
        step.store_result(
            {
                "Id": "001000000000000",
                "Lookup__c": "006000000000001",
                "Name": "Kara Thrace",
            }
        )
        oc.add_dependency.assert_called_once_with(
            "Opportunity", amaxa.SalesforceId("006000000000001")
        )
        oc.store_result.assert_called_once_with(
            "Contact",
            {
                "Id": "001000000000000",
                "Lookup__c": "006000000000001",
                "Name": "Kara Thrace",
            },
        )
        oc.add_dependency.reset_mock()
        oc.store_result.reset_mock()

        # Validate that the polymorphic lookup is treated properly when the content is a descendent reference
        step.store_result(
            {
                "Id": "001000000000000",
                "Lookup__c": "001000000000001",
                "Name": "Kara Thrace",
            }
        )
        oc.add_dependency.assert_not_called()
        oc.store_result.assert_called_once_with(
            "Contact",
            {
                "Id": "001000000000000",
                "Lookup__c": "001000000000001",
                "Name": "Kara Thrace",
            },
        )
        oc.add_dependency.reset_mock()
        oc.store_result.reset_mock()

        # Validate that the polymorphic lookup is treated properly when the content is a off-extraction reference
        step.store_result(
            {
                "Id": "001000000000000",
                "Lookup__c": "00T000000000001",
                "Name": "Kara Thrace",
            }
        )
        oc.add_dependency.assert_not_called()
        oc.store_result.assert_called_once_with(
            "Contact",
            {
                "Id": "001000000000000",
                "Lookup__c": "00T000000000001",
                "Name": "Kara Thrace",
            },
        )

    def test_store_result_handles_empty_lookup_values(self):
        connection = Mock()

        oc = amaxa.ExtractOperation(connection)

        oc.store_result = Mock()
        oc.add_dependency = Mock()
        oc.get_extracted_ids = Mock()
        oc.get_field_map = Mock(
            return_value={
                "Lookup__c": {
                    "name": "Lookup__c",
                    "type": "reference",
                    "referenceTo": ["Opportunity", "Account", "Task"],
                }
            }
        )
        oc.get_sobject_list = Mock(return_value=["Account", "Contact", "Opportunity"])
        oc.get_sobject_name_for_id = Mock()

        step = amaxa.ExtractionStep(
            "Contact", amaxa.ExtractionScope.ALL_RECORDS, ["Lookup__c"]
        )
        oc.add_step(step)
        step.initialize()

        # Validate that the polymorphic lookup is treated properly when the content is a dependent reference
        step.dependent_lookups = ["Lookup__c"]
        step.descendent_lookups = []
        step.self_lookups = []
        step.store_result(
            {"Id": "001000000000000", "Lookup__c": None, "Name": "Kara Thrace"}
        )
        oc.add_dependency.assert_not_called()
        oc.get_sobject_name_for_id.assert_not_called()
        oc.get_extracted_ids.assert_not_called()

        # Validate that the polymorphic lookup is treated properly when the content is a descendent reference
        step.dependent_lookups = []
        step.descendent_lookups = ["Lookup__c"]
        step.self_lookups = []
        step.store_result(
            {"Id": "001000000000000", "Lookup__c": None, "Name": "Kara Thrace"}
        )
        oc.add_dependency.assert_not_called()
        oc.get_sobject_name_for_id.assert_not_called()
        oc.get_extracted_ids.assert_not_called()

    def test_store_result_respects_outside_lookup_behavior_drop_field(self):
        connection = Mock()

        oc = amaxa.ExtractOperation(connection)

        oc.store_result = Mock()
        oc.add_dependency = Mock()
        oc.get_field_map = Mock(
            return_value={
                "AccountId": {
                    "name": "AccountId",
                    "type": "reference",
                    "referenceTo": ["Account"],
                },
                "LastName": {"name": "Name", "type": "string"},
            }
        )
        oc.get_sobject_list = Mock(return_value=["Account", "Contact"])
        oc.get_extracted_ids = Mock(return_value=set())

        step = amaxa.ExtractionStep(
            "Contact",
            amaxa.ExtractionScope.DESCENDENTS,
            ["AccountId"],
            outside_lookup_behavior=amaxa.OutsideLookupBehavior.DROP_FIELD,
        )

        oc.add_step(step)
        step.initialize()

        step.store_result({"Id": "003000000000001", "AccountId": "001000000000001"})
        oc.store_result.assert_called_once_with("Contact", {"Id": "003000000000001"})

    def test_store_result_respects_outside_lookup_behavior_error(self):
        connection = Mock()

        oc = amaxa.ExtractOperation(connection)

        oc.store_result = Mock()
        oc.add_dependency = Mock()
        oc.get_field_map = Mock(
            return_value={
                "AccountId": {
                    "name": "AccountId",
                    "type": "reference",
                    "referenceTo": ["Account"],
                },
                "LastName": {"name": "Name", "type": "string"},
            }
        )
        oc.get_sobject_list = Mock(return_value=["Account", "Contact"])
        oc.get_extracted_ids = Mock(return_value=set())

        step = amaxa.ExtractionStep(
            "Contact",
            amaxa.ExtractionScope.DESCENDENTS,
            ["AccountId"],
            outside_lookup_behavior=amaxa.OutsideLookupBehavior.ERROR,
        )

        oc.add_step(step)
        step.initialize()

        step.store_result({"Id": "003000000000001", "AccountId": "001000000000001"})
        self.assertEqual(
            [
                "{} {} has an outside reference in field {} ({}), which is not allowed by the extraction configuration.".format(
                    "Contact", "003000000000001", "AccountId", "001000000000001"
                )
            ],
            step.errors,
        )

    def test_store_result_respects_outside_lookup_behavior_include(self):
        connection = Mock()

        oc = amaxa.ExtractOperation(connection)

        oc.store_result = Mock()
        oc.add_dependency = Mock()
        oc.get_field_map = Mock(
            return_value={
                "AccountId": {
                    "name": "AccountId",
                    "type": "reference",
                    "referenceTo": ["Account"],
                },
                "LastName": {"name": "Name", "type": "string"},
            }
        )
        oc.get_sobject_list = Mock(return_value=["Account", "Contact"])
        oc.get_extracted_ids = Mock(return_value=set())

        step = amaxa.ExtractionStep(
            "Contact",
            amaxa.ExtractionScope.DESCENDENTS,
            ["AccountId"],
            outside_lookup_behavior=amaxa.OutsideLookupBehavior.INCLUDE,
        )

        oc.add_step(step)
        step.initialize()

        step.store_result({"Id": "003000000000001", "AccountId": "001000000000001"})
        oc.store_result.assert_called_once_with(
            "Contact", {"Id": "003000000000001", "AccountId": "001000000000001"}
        )

    def test_store_result_discriminates_polymorphic_lookup_type(self):
        connection = Mock()

        oc = amaxa.ExtractOperation(connection)

        oc.store_result = Mock()
        oc.add_dependency = Mock()
        oc.get_field_map = Mock(
            return_value={
                "AccountId": {
                    "name": "AccountId",
                    "type": "reference",
                    "referenceTo": ["Account"],
                },
                "WhoId": {"name": "Name", "type": "string"},
            }
        )
        oc.get_sobject_list = Mock(return_value=["Account", "Contact", "Task"])
        oc.get_extracted_ids = Mock(return_value=set())

        step = amaxa.ExtractionStep(
            "Contact",
            amaxa.ExtractionScope.DESCENDENTS,
            ["AccountId"],
            outside_lookup_behavior=amaxa.OutsideLookupBehavior.DROP_FIELD,
        )

        oc.add_step(step)
        step.initialize()

        step.store_result({"Id": "003000000000001", "AccountId": "001000000000001"})
        oc.store_result.assert_called_once_with("Contact", {"Id": "003000000000001"})

    def test_perform_lookup_pass_executes_correct_query(self):
        connection = Mock()

        oc = amaxa.ExtractOperation(connection)

        oc.get_field_map = Mock(
            return_value={
                "Lookup__c": {
                    "name": "Lookup__c",
                    "type": "reference",
                    "referenceTo": ["Account"],
                }
            }
        )
        oc.get_sobject_ids_for_reference = Mock(
            return_value=set([amaxa.SalesforceId("001000000000000")])
        )

        step = amaxa.ExtractionStep(
            "Account", amaxa.ExtractionScope.ALL_RECORDS, ["Lookup__c"]
        )
        oc.add_step(step)
        step.initialize()

        step.perform_id_field_pass = Mock()
        step.perform_lookup_pass("Lookup__c")

        oc.get_sobject_ids_for_reference.assert_called_once_with("Account", "Lookup__c")
        step.perform_id_field_pass.assert_called_once_with(
            "Lookup__c", set([amaxa.SalesforceId("001000000000000")])
        )

    def test_perform_id_field_pass_queries_all_records(self):
        connection = Mock()
        connection.query_all = Mock(
            side_effect=lambda x: {"records": [{"Id": "001000000000001"}]}
        )

        oc = amaxa.ExtractOperation(connection)
        oc.get_field_map = Mock(
            return_value={
                "Lookup__c": {
                    "name": "Lookup__c",
                    "type": "reference",
                    "referenceTo": ["Account"],
                }
            }
        )

        step = amaxa.ExtractionStep(
            "Account", amaxa.ExtractionScope.ALL_RECORDS, ["Lookup__c"]
        )
        step.store_result = Mock()
        oc.add_step(step)
        step.initialize()

        id_set = set()
        # Generate enough fake Ids to require two queries.
        for i in range(400):
            new_id = amaxa.SalesforceId("001000000000" + str(i + 1).zfill(3))
            id_set.add(new_id)

        self.assertEqual(400, len(id_set))

        step.perform_id_field_pass("Lookup__c", id_set)

        self.assertLess(1, len(connection.query_all.call_args_list))
        total = 0
        for call in connection.query_all.call_args_list:
            self.assertLess(len(call[0][0]) - call[0][0].find("WHERE"), 4000)
            total += call[0][0].count("'001")
        self.assertEqual(400, total)

    def test_perform_id_field_pass_stores_results(self):
        connection = Mock()
        connection.query_all = Mock(
            side_effect=lambda x: {
                "records": [{"Id": "001000000000001"}, {"Id": "001000000000002"}]
            }
        )

        oc = amaxa.ExtractOperation(connection)
        oc.get_field_map = Mock(
            return_value={
                "Lookup__c": {
                    "name": "Lookup__c",
                    "type": "reference",
                    "referenceTo": ["Account"],
                }
            }
        )

        step = amaxa.ExtractionStep(
            "Account", amaxa.ExtractionScope.ALL_RECORDS, ["Lookup__c"]
        )
        step.store_result = Mock()
        oc.add_step(step)
        step.initialize()

        step.perform_id_field_pass(
            "Lookup__c",
            set(
                [
                    amaxa.SalesforceId("001000000000001"),
                    amaxa.SalesforceId("001000000000002"),
                ]
            ),
        )
        step.store_result.assert_any_call(connection.query_all("Account")["records"][0])
        step.store_result.assert_any_call(connection.query_all("Account")["records"][1])

    def test_perform_id_field_pass_ignores_empty_set(self):
        connection = Mock()

        oc = amaxa.ExtractOperation(connection)
        oc.get_field_map = Mock(
            return_value={
                "Lookup__c": {
                    "name": "Lookup__c",
                    "type": "reference",
                    "referenceTo": ["Account"],
                }
            }
        )

        step = amaxa.ExtractionStep(
            "Account", amaxa.ExtractionScope.ALL_RECORDS, ["Lookup__c"]
        )
        oc.add_step(step)
        step.initialize()

        step.perform_id_field_pass("Lookup__c", set())

        connection.query_all.assert_not_called()

    @patch("amaxa.ExtractOperation.bulk", new_callable=PropertyMock())
    def test_perform_bulk_api_pass_calls_query(self, bulk_proxy):
        connection = Mock()
        oc = amaxa.ExtractOperation(connection)
        oc.get_field_map = Mock(
            return_value={
                "Lookup__c": {
                    "name": "Lookup__c",
                    "type": "reference",
                    "referenceTo": ["Account"],
                }
            }
        )
        retval = [{"Id": "001000000000001"}, {"Id": "001000000000002"}]
        bulk_proxy.is_batch_done = Mock(side_effect=[False, True])
        bulk_proxy.create_query_job = Mock(return_value="075000000000000AAA")
        bulk_proxy.get_all_results_for_query_batch = Mock(
            return_value=[IteratorBytesIO([json.dumps(retval).encode("utf-8")])]
        )
        step = amaxa.ExtractionStep(
            "Account", amaxa.ExtractionScope.QUERY, ["Lookup__c"]
        )
        step.store_result = Mock()
        oc.add_step(step)
        step.initialize()

        step.perform_bulk_api_pass("SELECT Id FROM Account")
        bulk_proxy.query.assert_called_once_with(
            "075000000000000AAA", "SELECT Id FROM Account"
        )

    @patch("amaxa.ExtractOperation.bulk", new_callable=PropertyMock())
    @patch.object(amaxa, "sleep")
    def test_perform_bulk_api_pass_waits_as_configured(self, time_proxy, bulk_proxy):
        connection = Mock()
        oc = amaxa.ExtractOperation(connection)
        oc.get_field_map = Mock(
            return_value={
                "Lookup__c": {
                    "name": "Lookup__c",
                    "type": "reference",
                    "referenceTo": ["Account"],
                }
            }
        )
        retval = [{"Id": "001000000000001"}, {"Id": "001000000000002"}]
        bulk_proxy.is_batch_done = Mock(side_effect=[False, True])
        bulk_proxy.create_query_job = Mock(return_value="075000000000000AAA")
        bulk_proxy.get_all_results_for_query_batch = Mock(
            return_value=[IteratorBytesIO([json.dumps(retval).encode("utf-8")])]
        )
        step = amaxa.ExtractionStep(
            "Account", amaxa.ExtractionScope.QUERY, ["Lookup__c"],
            options={"bulk-api-poll-interval": 20}
        )
        step.store_result = Mock()
        oc.add_step(step)
        step.initialize()

        step.perform_bulk_api_pass("SELECT Id FROM Account")
        time_proxy.assert_called_once_with(20)

    @patch("amaxa.ExtractOperation.bulk", new_callable=PropertyMock())
    def test_perform_bulk_api_pass_stores_results(self, bulk_proxy):
        connection = Mock()

        oc = amaxa.ExtractOperation(connection)
        oc.get_field_map = Mock(
            return_value={
                "Lookup__c": {
                    "name": "Lookup__c",
                    "type": "reference",
                    "referenceTo": ["Account"],
                }
            }
        )
        retval = [{"Id": "001000000000001"}, {"Id": "001000000000002"}]
        bulk_proxy.is_batch_done = Mock(side_effect=[False, True])
        bulk_proxy.create_query_job = Mock(return_value="075000000000000AAA")
        bulk_proxy.get_all_results_for_query_batch = Mock(
            return_value=[IteratorBytesIO([json.dumps(retval).encode("utf-8")])]
        )

        step = amaxa.ExtractionStep(
            "Account", amaxa.ExtractionScope.ALL_RECORDS, ["Lookup__c"]
        )
        step.store_result = Mock()
        oc.add_step(step)
        step.initialize()

        step.perform_bulk_api_pass("SELECT Id FROM Account")
        step.store_result.assert_any_call(retval[0])
        step.store_result.assert_any_call(retval[1])

    @patch("amaxa.ExtractOperation.bulk", new_callable=PropertyMock())
    def test_perform_bulk_api_pass_stores_high_volume_results(self, bulk_proxy):
        connection = Mock()

        oc = amaxa.ExtractOperation(connection)
        oc.get_field_map = Mock(
            return_value={
                "Lookup__c": {
                    "name": "Lookup__c",
                    "type": "reference",
                    "referenceTo": ["Account"],
                }
            }
        )
        retval = []
        for i in range(5):
            retval.append(
                [
                    {
                        "Id": "00100000{:d}{:06d}".format(i, j),
                        "Name": "Account {:d}{:06d}".format(i, j),
                    }
                    for j in range(100000)
                ]
            )

        bulk_proxy.is_batch_done = Mock(side_effect=[False, True])
        bulk_proxy.create_query_job = Mock(return_value="075000000000000AAA")
        bulk_proxy.get_all_results_for_query_batch = Mock(
            return_value=[
                IteratorBytesIO([json.dumps(chunk).encode("utf-8")]) for chunk in retval
            ]
        )

        step = amaxa.ExtractionStep(
            "Account", amaxa.ExtractionScope.ALL_RECORDS, ["Lookup__c"]
        )
        step.store_result = Mock()
        oc.add_step(step)
        step.initialize()

        step.perform_bulk_api_pass("SELECT Id FROM Account")
        self.assertEqual(500000, step.store_result.call_count)

    @patch("amaxa.ExtractOperation.bulk", new_callable=PropertyMock())
    def test_perform_bulk_api_pass_converts_datetimes(self, bulk_proxy):
        connection = Mock()

        oc = amaxa.ExtractOperation(connection)
        oc.get_field_map = Mock(
            return_value={"CreatedDate": {"name": "CreatedDate", "type": "datetime"}}
        )
        retval = [{"Id": "001000000000001", "CreatedDate": 1546659665000}]
        bulk_proxy.is_batch_done = Mock(side_effect=[False, True])
        bulk_proxy.create_query_job = Mock(return_value="075000000000000AAA")
        bulk_proxy.get_all_results_for_query_batch = Mock(
            return_value=[IteratorBytesIO([json.dumps(retval).encode("utf-8")])]
        )

        step = amaxa.ExtractionStep(
            "Account", amaxa.ExtractionScope.QUERY, ["CreatedDate"]
        )
        step.store_result = Mock()
        oc.add_step(step)
        step.initialize()

        step.perform_bulk_api_pass("SELECT Id, CreatedDate FROM Account")
        step.store_result.assert_called_once_with(
            {"Id": "001000000000001", "CreatedDate": "2019-01-05T03:41:05.000+0000"}
        )

    def test_resolve_registered_dependencies_loads_records(self):
        connection = Mock()

        oc = amaxa.ExtractOperation(connection)
        oc.get_field_map = Mock(
            return_value={
                "Lookup__c": {
                    "name": "Lookup__c",
                    "type": "reference",
                    "referenceTo": ["Account"],
                }
            }
        )
        oc.get_dependencies = Mock(
            side_effect=[
                set(
                    [
                        amaxa.SalesforceId("001000000000001"),
                        amaxa.SalesforceId("001000000000002"),
                    ]
                ),
                set(),
            ]
        )

        step = amaxa.ExtractionStep(
            "Account", amaxa.ExtractionScope.ALL_RECORDS, ["Lookup__c"]
        )
        step.perform_id_field_pass = Mock()
        oc.add_step(step)
        step.initialize()

        step.resolve_registered_dependencies()

        oc.get_dependencies.assert_has_calls(
            [unittest.mock.call("Account"), unittest.mock.call("Account")]
        )
        step.perform_id_field_pass.assert_called_once_with(
            "Id",
            set(
                [
                    amaxa.SalesforceId("001000000000001"),
                    amaxa.SalesforceId("001000000000002"),
                ]
            ),
        )

    def test_resolve_registered_dependencies_registers_error_for_missing_ids(self):
        connection = Mock()

        oc = amaxa.ExtractOperation(connection)
        oc.get_field_map = Mock(
            return_value={
                "Lookup__c": {
                    "name": "Lookup__c",
                    "type": "reference",
                    "referenceTo": ["Account"],
                }
            }
        )
        oc.get_dependencies = Mock(
            side_effect=[
                set(
                    [
                        amaxa.SalesforceId("001000000000001"),
                        amaxa.SalesforceId("001000000000002"),
                    ]
                ),
                set([amaxa.SalesforceId("001000000000002")]),
            ]
        )

        step = amaxa.ExtractionStep(
            "Account", amaxa.ExtractionScope.ALL_RECORDS, ["Lookup__c"]
        )
        step.perform_id_field_pass = Mock()
        oc.add_step(step)
        step.initialize()

        step.resolve_registered_dependencies()
        self.assertEqual(
            [
                "Unable to resolve dependencies for sObject {}. The following Ids could not be found: {}".format(
                    step.sobjectname,
                    ", ".join(
                        [str(i) for i in [amaxa.SalesforceId("001000000000002")]]
                    ),
                )
            ],
            step.errors,
        )

    def test_execute_with_all_records_performs_bulk_api_pass(self):
        connection = Mock()

        oc = amaxa.ExtractOperation(connection)
        oc.get_field_map = Mock(return_value={"Name": {"name": "Name", "type": "text"}})

        step = amaxa.ExtractionStep(
            "Account", amaxa.ExtractionScope.ALL_RECORDS, ["Name"]
        )
        step.perform_bulk_api_pass = Mock()
        oc.add_step(step)

        step.initialize()
        step.execute()

        step.perform_bulk_api_pass.assert_called_once_with("SELECT Name FROM Account")

    def test_execute_with_query_performs_bulk_api_pass(self):
        connection = Mock()

        oc = amaxa.ExtractOperation(connection)
        oc.get_field_map = Mock(return_value={"Name": {"name": "Name", "type": "text"}})

        step = amaxa.ExtractionStep(
            "Account", amaxa.ExtractionScope.QUERY, ["Name"], "Name != null"
        )
        step.perform_bulk_api_pass = Mock()
        oc.add_step(step)

        step.initialize()
        step.execute()

        step.perform_bulk_api_pass.assert_called_once_with(
            "SELECT Name FROM Account WHERE Name != null"
        )

    def test_execute_loads_all_descendents(self):
        connection = Mock()

        oc = amaxa.ExtractOperation(connection)
        oc.get_field_map = Mock(
            return_value={
                "Name": {"name": "Name", "type": "text"},
                "AccountId": {
                    "name": "AccountId",
                    "type": "reference",
                    "referenceTo": ["Account"],
                },
                "Household__c": {
                    "name": "Household__c",
                    "type": "reference",
                    "referenceTo": ["Account"],
                },
                "Event__c": {
                    "name": "Event__c",
                    "type": "reference",
                    "referenceTo": ["Event__c"],
                },
            }
        )
        oc.get_sobject_list = Mock(return_value=["Account", "Contact"])

        step = amaxa.ExtractionStep(
            "Contact",
            amaxa.ExtractionScope.DESCENDENTS,
            ["Name", "AccountId", "Household__c"],
        )
        step.perform_lookup_pass = Mock()
        oc.add_step(step)

        step.initialize()
        step.execute()

        step.perform_lookup_pass.assert_has_calls(
            [unittest.mock.call("AccountId"), unittest.mock.call("Household__c")],
            any_order=True,
        )

    def test_execute_resolves_self_lookups(self):
        connection = Mock()

        oc = amaxa.ExtractOperation(connection)
        oc.get_field_map = Mock(
            return_value={
                "Name": {"name": "Name", "type": "text"},
                "ParentId": {
                    "name": "ParentId",
                    "type": "reference",
                    "referenceTo": ["Account"],
                },
            }
        )
        oc.get_extracted_ids = Mock(
            side_effect=[
                set([amaxa.SalesforceId("001000000000001")]),
                set(
                    [
                        amaxa.SalesforceId("001000000000001"),
                        amaxa.SalesforceId("001000000000002"),
                    ]
                ),
                set(
                    [
                        amaxa.SalesforceId("001000000000001"),
                        amaxa.SalesforceId("001000000000002"),
                    ]
                ),
                set(
                    [
                        amaxa.SalesforceId("001000000000001"),
                        amaxa.SalesforceId("001000000000002"),
                    ]
                ),
            ]
        )

        step = amaxa.ExtractionStep(
            "Account",
            amaxa.ExtractionScope.QUERY,
            ["Name", "ParentId"],
            "Name = 'ACME'",
        )
        step.perform_bulk_api_pass = Mock()
        step.perform_lookup_pass = Mock()
        step.resolve_registered_dependencies = Mock()
        oc.add_step(step)

        step.initialize()
        self.assertEqual(set(["ParentId"]), step.self_lookups)

        step.execute()

        step.perform_bulk_api_pass.assert_called_once_with(
            "SELECT Name, ParentId FROM Account WHERE Name = 'ACME'"
        )
        oc.get_extracted_ids.assert_has_calls(
            [
                unittest.mock.call("Account"),
                unittest.mock.call("Account"),
                unittest.mock.call("Account"),
                unittest.mock.call("Account"),
            ]
        )
        step.perform_lookup_pass.assert_has_calls(
            [unittest.mock.call("ParentId"), unittest.mock.call("ParentId")]
        )
        step.resolve_registered_dependencies.assert_has_calls(
            [unittest.mock.call(), unittest.mock.call()]
        )

    def test_execute_does_not_trace_self_lookups_without_trace_all(self):
        connection = Mock()

        oc = amaxa.ExtractOperation(connection)
        oc.get_field_map = Mock(
            return_value={
                "Name": {"name": "Name", "type": "text"},
                "ParentId": {
                    "name": "ParentId",
                    "type": "reference",
                    "referenceTo": ["Account"],
                },
            }
        )
        oc.get_extracted_ids = Mock()

        step = amaxa.ExtractionStep(
            "Account",
            amaxa.ExtractionScope.QUERY,
            ["Name", "ParentId"],
            "Name = 'ACME'",
            amaxa.SelfLookupBehavior.TRACE_NONE,
        )

        step.perform_bulk_api_pass = Mock()
        step.perform_lookup_pass = Mock()
        step.resolve_registered_dependencies = Mock()

        oc.add_step(step)

        step.initialize()
        step.execute()

        self.assertEqual(set(["ParentId"]), step.self_lookups)
        step.resolve_registered_dependencies.assert_called_once_with()
        oc.get_extracted_ids.assert_not_called()
