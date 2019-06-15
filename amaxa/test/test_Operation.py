import unittest
from unittest.mock import Mock, MagicMock, PropertyMock, patch
from .. import amaxa


class test_Operation(unittest.TestCase):
    def test_stores_steps(self):
        connection = Mock()
        oc = amaxa.Operation(connection)

        step = Mock()
        oc.add_step(step)

        self.assertEqual([step], oc.steps)
        self.assertEqual(oc, step.context)

    def test_creates_and_caches_proxy_objects(self):
        connection = Mock()
        p = PropertyMock(return_value="Account")
        type(connection).Account = p

        oc = amaxa.Operation(connection)

        proxy = oc.get_proxy_object("Account")

        self.assertEqual("Account", proxy)
        p.assert_called_once_with()

        p.reset_mock()
        proxy = oc.get_proxy_object("Account")

        # Proxy should be cached
        self.assertEqual("Account", proxy)
        p.assert_not_called()

    @patch("salesforce_bulk.SalesforceBulk")
    def test_creates_and_caches_bulk_proxy_object(self, bulk_proxy):
        connection = Mock(session_id="000", bulk_url="https://login.salesforce.com")
        oc = amaxa.Operation(connection)

        b = oc.bulk
        bulk_proxy.assert_called_once_with(sessionId="000", host="login.salesforce.com")

        bulk_proxy.reset_mock()
        b = oc.bulk

        # Proxy should be cached
        bulk_proxy.assert_not_called()

    @patch("amaxa.Operation.get_proxy_object")
    def test_caches_describe_results(self, proxy_mock):
        connection = Mock()
        account_mock = Mock()

        fields = [{"name": "Name"}, {"name": "Id"}]
        describe_info = {"fields": fields}

        account_mock.describe = Mock(return_value=describe_info)
        proxy_mock.return_value = account_mock

        oc = amaxa.Operation(connection)

        retval = oc.get_describe("Account")
        self.assertEqual(describe_info, retval)
        account_mock.describe.assert_called_once_with()
        account_mock.describe.reset_mock()

        retval = oc.get_describe("Account")
        self.assertEqual(describe_info, retval)
        account_mock.describe.assert_not_called()

    @patch("amaxa.Operation.get_proxy_object")
    def test_caches_field_maps(self, proxy_mock):
        connection = Mock()
        account_mock = Mock()

        fields = [{"name": "Name"}, {"name": "Id"}]
        describe_info = {"fields": fields}

        account_mock.describe = Mock(return_value=describe_info)
        proxy_mock.return_value = account_mock

        oc = amaxa.Operation(connection)

        retval = oc.get_field_map("Account")
        self.assertEqual({"Name": {"name": "Name"}, "Id": {"name": "Id"}}, retval)
        account_mock.describe.assert_called_once_with()
        account_mock.describe.reset_mock()

        retval = oc.get_field_map("Account")
        self.assertEqual({"Name": {"name": "Name"}, "Id": {"name": "Id"}}, retval)
        account_mock.describe.assert_not_called()

    @patch("amaxa.Operation.get_proxy_object")
    def test_filters_field_maps(self, proxy_mock):
        connection = Mock()
        account_mock = Mock()

        fields = [{"name": "Name"}, {"name": "Id"}]
        describe_info = {"fields": fields}

        account_mock.describe = Mock(return_value=describe_info)
        proxy_mock.return_value = account_mock

        oc = amaxa.Operation(connection)

        retval = oc.get_filtered_field_map("Account", lambda f: f["name"] == "Id")
        self.assertEqual({"Id": {"name": "Id"}}, retval)

    def test_maps_ids_to_sobject_types(self):
        connection = Mock()
        connection.describe = Mock(
            return_value={
                "sobjects": [
                    {"name": "Account", "keyPrefix": "001"},
                    {"name": "Contact", "keyPrefix": "003"},
                ]
            }
        )

        oc = amaxa.Operation(connection)

        self.assertEqual("Account", oc.get_sobject_name_for_id("001000000000000"))
        self.assertEqual("Contact", oc.get_sobject_name_for_id("003000000000000"))

        connection.describe.assert_called_once_with()

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
