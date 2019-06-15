import unittest
from unittest.mock import Mock, MagicMock, PropertyMock, patch
from .. import amaxa
from .MockFileStore import MockFileStore


class test_ExtractOperation(unittest.TestCase):
    def test_execute_runs_all_steps(self):
        connection = Mock()
        oc = amaxa.ExtractOperation(connection)

        # pylint: disable=W0612
        for i in range(3):
            oc.add_step(Mock(sobjectname=str(i), errors=[]))

        oc.execute()

        for s in oc.steps:
            s.execute.assert_called_once_with()
            self.assertEqual(oc, s.context)

    def test_add_dependency_tracks_dependencies(self):
        connection = Mock()

        oc = amaxa.ExtractOperation(connection)

        self.assertEqual(set(), oc.get_dependencies("Account"))
        oc.add_dependency("Account", amaxa.SalesforceId("001000000000000"))
        self.assertEqual(
            set([amaxa.SalesforceId("001000000000000")]), oc.get_dependencies("Account")
        )

    def test_add_dependency_ignores_extracted_record(self):
        connection = Mock()

        oc = amaxa.ExtractOperation(connection)
        oc.file_store = MockFileStore()

        oc.store_result("Account", {"Id": "001000000000000", "Name": "Caprica Steel"})
        self.assertEqual(set(), oc.get_dependencies("Account"))
        oc.add_dependency("Account", amaxa.SalesforceId("001000000000000"))
        self.assertEqual(set(), oc.get_dependencies("Account"))

    def test_store_result_retains_ids(self):
        connection = Mock()

        oc = amaxa.ExtractOperation(connection)
        oc.file_store = MockFileStore()

        oc.store_result("Account", {"Id": "001000000000000", "Name": "Caprica Steel"})
        self.assertEqual(
            set([amaxa.SalesforceId("001000000000000")]), oc.extracted_ids["Account"]
        )

    def test_store_result_writes_records(self):
        connection = Mock()

        oc = amaxa.ExtractOperation(connection)
        oc.file_store = MockFileStore()

        oc.store_result("Account", {"Id": "001000000000000", "Name": "Caprica Steel"})
        oc.file_store.get_csv(
            "Account", amaxa.FileType.OUTPUT
        ).writerow.assert_called_once_with(
            {"Id": "001000000000000", "Name": "Caprica Steel"}
        )

    def test_store_result_transforms_output(self):
        connection = Mock()

        oc = amaxa.ExtractOperation(connection)
        oc.file_store = MockFileStore()
        mapper_mock = Mock()
        mapper_mock.transform_record = Mock(
            return_value={"Id": "001000000000000", "Name": "Caprica City Steel"}
        )

        oc.mappers["Account"] = mapper_mock

        oc.store_result("Account", {"Id": "001000000000000", "Name": "Caprica Steel"})
        mapper_mock.transform_record.assert_called_once_with(
            {"Id": "001000000000000", "Name": "Caprica Steel"}
        )
        oc.file_store.get_csv(
            "Account", amaxa.FileType.OUTPUT
        ).writerow.assert_called_once_with(
            {"Id": "001000000000000", "Name": "Caprica City Steel"}
        )

    def test_store_result_clears_dependencies(self):
        connection = Mock()

        oc = amaxa.ExtractOperation(connection)
        oc.file_store = MockFileStore()
        oc.add_dependency("Account", amaxa.SalesforceId("001000000000000"))

        oc.store_result("Account", {"Id": "001000000000000", "Name": "Caprica Steel"})
        self.assertEqual(set(), oc.get_dependencies("Account"))

    def test_store_result_does_not_write_duplicate_records(self):
        connection = Mock()
        oc = amaxa.ExtractOperation(connection)
        oc.file_store = MockFileStore()

        oc.store_result("Account", {"Id": "001000000000000", "Name": "Caprica Steel"})
        oc.file_store.get_csv(
            "Account", amaxa.FileType.OUTPUT
        ).writerow.assert_called_once_with(
            {"Id": "001000000000000", "Name": "Caprica Steel"}
        )
        oc.file_store.get_csv("Account", amaxa.FileType.OUTPUT).writerow.reset_mock()
        oc.store_result("Account", {"Id": "001000000000000", "Name": "Caprica Steel"})
        oc.file_store.get_csv(
            "Account", amaxa.FileType.OUTPUT
        ).writerow.assert_not_called()

    def test_get_extracted_ids_returns_results(self):
        connection = Mock()
        oc = amaxa.ExtractOperation(connection)
        oc.file_store = MockFileStore()

        oc.store_result("Account", {"Id": "001000000000000", "Name": "Caprica Steel"})
        self.assertEqual(
            set([amaxa.SalesforceId("001000000000000")]),
            oc.get_extracted_ids("Account"),
        )

    def test_get_sobject_ids_for_reference_returns_correct_ids(self):
        connection = Mock()

        oc = amaxa.ExtractOperation(connection)
        oc.file_store = MockFileStore()
        oc.get_field_map = Mock(
            return_value={"Lookup__c": {"referenceTo": ["Account", "Contact"]}}
        )

        oc.store_result(
            "Account", {"Id": "001000000000000", "Name": "University of Caprica"}
        )
        oc.store_result("Contact", {"Id": "003000000000000", "Name": "Gaius Baltar"})
        oc.store_result(
            "Opportunity", {"Id": "006000000000000", "Name": "Defense Mainframe"}
        )

        self.assertEqual(
            set(
                [
                    amaxa.SalesforceId("001000000000000"),
                    amaxa.SalesforceId("003000000000000"),
                ]
            ),
            oc.get_sobject_ids_for_reference("Account", "Lookup__c"),
        )
