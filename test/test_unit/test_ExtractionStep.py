import unittest
from unittest.mock import Mock

import amaxa

from .MockConnection import MockConnection


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

    def test_perform_bulk_api_pass_stores_results(self):
        retval = [{"Id": "001000000000001"}, {"Id": "001000000000002"}]
        connection = MockConnection(bulk_query_results=retval)

        oc = amaxa.ExtractOperation(connection)

        step = amaxa.ExtractionStep(
            "Account", amaxa.ExtractionScope.ALL_RECORDS, ["Name"]
        )
        step.store_result = Mock()
        oc.add_step(step)
        step.initialize()

        step.perform_bulk_api_pass("SELECT Id FROM Account")
        step.store_result.assert_any_call(retval[0])
        step.store_result.assert_any_call(retval[1])

    def test_resolve_registered_dependencies_loads_records(self):
        oc = Mock()
        id_set = set(
            [
                amaxa.SalesforceId("001000000000001"),
                amaxa.SalesforceId("001000000000002"),
            ]
        )
        oc.get_dependencies = Mock(
            side_effect=[id_set, set([amaxa.SalesforceId("001000000000002")]),]
        )
        oc.connection.retrieve_records_by_id = Mock(
            return_value=[{"Id": amaxa.SalesforceId("001000000000001")}]
        )

        step = amaxa.ExtractionStep(
            "Account", amaxa.ExtractionScope.ALL_RECORDS, ["Id"]
        )
        step.context = oc
        step.store_result = Mock()

        step.resolve_registered_dependencies()

        oc.connection.retrieve_records_by_id.assert_called_once_with(
            "Account", id_set, ["Id"]
        )
        step.store_result.assert_called_once_with(
            {"Id": amaxa.SalesforceId("001000000000001")}
        )
        oc.get_dependencies.assert_has_calls(
            [unittest.mock.call("Account"), unittest.mock.call("Account")]
        )

        assert step.errors == [
            "Unable to resolve dependencies for sObject Account. The following Ids could not be found: 001000000000002AAA"
        ]

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
        connection.retrieve_records_by_id = Mock(return_value=[])
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
        connection.bulk_api_query.return_value = []
        connection.retrieve_records_by_id.return_value = []

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
        connection.retrieve_records_by_id.return_value = []

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
