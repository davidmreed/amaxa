import unittest
from unittest.mock import Mock

import amaxa


class test_Operation(unittest.TestCase):
    def test_stores_steps(self):
        connection = Mock()
        oc = amaxa.Operation(connection)

        step = Mock()
        oc.add_step(step)

        self.assertEqual([step], oc.steps)
        self.assertEqual(oc, step.context)

    def test_caches_describe_results(self):
        connection = Mock()

        fields = [{"name": "Name"}, {"name": "Id"}]
        describe_info = {"fields": fields}

        connection.get_sobject_describe.return_value = describe_info
        oc = amaxa.Operation(connection)

        retval = oc.get_describe("Account")
        self.assertEqual(describe_info, retval)
        connection.get_sobject_describe.assert_called_once_with("Account")
        connection.get_sobject_describe.reset_mock()

        retval = oc.get_describe("Account")
        self.assertEqual(describe_info, retval)
        connection.get_sobject_describe.assert_not_called()

    def test_caches_field_maps(self):
        connection = Mock()

        fields = [{"name": "Name"}, {"name": "Id"}]
        describe_info = {"fields": fields}

        connection.get_sobject_describe.return_value = describe_info
        oc = amaxa.Operation(connection)

        retval = oc.get_field_map("Account")
        self.assertEqual({"Name": {"name": "Name"}, "Id": {"name": "Id"}}, retval)
        connection.get_sobject_describe.assert_called_once_with("Account")
        connection.get_sobject_describe.reset_mock()

        retval = oc.get_field_map("Account")
        self.assertEqual({"Name": {"name": "Name"}, "Id": {"name": "Id"}}, retval)
        connection.get_sobject_describe.assert_not_called()

    def test_filters_field_maps(self):
        connection = Mock()

        fields = [{"name": "Name"}, {"name": "Id"}]
        describe_info = {"fields": fields}

        connection.get_sobject_describe.return_value = describe_info
        oc = amaxa.Operation(connection)

        retval = oc.get_filtered_field_map("Account", lambda f: f["name"] == "Id")
        self.assertEqual({"Id": {"name": "Id"}}, retval)

    def test_maps_ids_to_sobject_types(self):
        connection = Mock()
        connection.get_global_describe.return_value = {
            "sobjects": [
                {"name": "Account", "keyPrefix": "001"},
                {"name": "Contact", "keyPrefix": "003"},
            ]
        }

        oc = amaxa.Operation(connection)

        self.assertEqual("Account", oc.get_sobject_name_for_id("001000000000000"))
        self.assertEqual("Contact", oc.get_sobject_name_for_id("003000000000000"))

        connection.get_global_describe.assert_called_once_with()

    def test_run_calls_initialize_and_execute(self):
        connection = Mock()
        op = amaxa.Operation(connection)
        op.initialize = Mock()
        op.execute = Mock(return_value=0)
        op.file_store = Mock()

        self.assertEqual(0, op.run())
        op.initialize.assert_called_once_with()
        op.execute.assert_called_once_with()
        op.file_store.close.assert_called_once_with()

    def test_run_logs_exceptions(self):
        connection = Mock()
        op = amaxa.Operation(connection)
        op.initialize = Mock()
        op.execute = Mock(side_effect=amaxa.AmaxaException("Test"))
        op.logger = Mock()
        op.file_store = Mock()

        self.assertEqual(-1, op.run())

        op.logger.error.assert_called_once_with("Unexpected exception Test occurred.")
        op.file_store.close.assert_called_once_with()
