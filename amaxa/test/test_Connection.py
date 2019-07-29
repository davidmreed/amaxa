import json
import unittest
from unittest.mock import Mock, call, patch
from salesforce_bulk.util import IteratorBytesIO
from ..api import Connection
from .. import amaxa


class test_Connection(unittest.TestCase):
    @patch("salesforce_bulk.SalesforceBulk")
    def test_init_creates_bulk_instance(self, bulk_mock):
        sf = Mock()
        sf.bulk_url = "https://salesforce.com"

        Connection(sf)

        bulk_mock.assert_called_once_with(
            sessionId=sf.session_id, host="salesforce.com", API_version=amaxa.constants.API_VERSION
        )

    def test_get_global_describe_calls_salesforce(self):
        sf = Mock()
        sf.bulk_url = "https://salesforce.com"

        conn = Connection(sf)
        self.assertEqual(sf.describe.return_value, conn.get_global_describe())

        sf.describe.assert_called_once_with()

    def test_get_sobject_describe_calls_salesforce(self):
        sf = Mock()
        sf.bulk_url = "https://salesforce.com"

        conn = Connection(sf)
        self.assertEqual(
            sf.Account.describe.return_value, conn.get_sobject_describe("Account")
        )
        sf.Account.describe.assert_called_once_with()

    def test_bulk_api_query(self):  # FIXME: test wait
        sf = Mock()
        sf.bulk_url = "https://salesforce.com"

        conn = Connection(sf)
        conn._bulk = Mock()

        retval = [{"Id": "001000000000001"}, {"Id": "001000000000002"}]
        conn._bulk.is_batch_done = Mock(side_effect=[False, True])
        conn._bulk.create_query_job = Mock(return_value="075000000000000AAA")
        conn._bulk.get_all_results_for_query_batch = Mock(
            return_value=[IteratorBytesIO([json.dumps(retval).encode("utf-8")])]
        )

        results = list(conn.bulk_api_query("Account", "SELECT Id FROM Account", [], 5))
        conn._bulk.query.assert_called_once_with(
            "075000000000000AAA", "SELECT Id FROM Account"
        )
        self.assertEqual(
            conn._bulk.is_batch_done.call_args_list,
            [call(conn._bulk.query.return_value), call(conn._bulk.query.return_value)],
        )
        conn._bulk.get_all_results_for_query_batch.assert_called_once_with(
            conn._bulk.query.return_value
        )

        self.assertEqual(retval, results)

    def test_bulk_query_converts_datetimes(self):
        sf = Mock()
        sf.bulk_url = "https://salesforce.com"

        conn = Connection(sf)
        conn._bulk = Mock()

        retval = [
            {"Id": "001000000000001", "CreatedDate": 1546659665000},
            {"Id": "001000000000002", "CreatedDate": None},
        ]
        conn._bulk.is_batch_done = Mock(side_effect=[False, True])
        conn._bulk.create_query_job = Mock(return_value="075000000000000AAA")
        conn._bulk.get_all_results_for_query_batch = Mock(
            return_value=[IteratorBytesIO([json.dumps(retval).encode("utf-8")])]
        )

        results = list(
            conn.bulk_api_query(
                "Account", "SELECT Id, CreatedDate, FROM Account", ["CreatedDate"], 5
            )
        )

        self.assertEqual(
            results[0],
            {"Id": "001000000000001", "CreatedDate": "2019-01-05T03:41:05.000+0000"},
        )
        self.assertEqual(results[1], {"Id": "001000000000002", "CreatedDate": None})

    def test_bulk_api_insert(self):
        sf = Mock()
        sf.bulk_url = "https://salesforce.com"

        conn = Connection(sf)
        conn._bulk = Mock()
        conn._bulk_api_insert_update = Mock(return_value=[])

        with patch("builtins.iter") as iter_mock:
            self.assertEqual([], list(conn.bulk_api_insert("Account", [], 120, 5, 1)))

            conn._bulk.create_insert_job.assert_called_once_with(
                "Account", contentType="JSON"
            )

            conn._bulk_api_insert_update.assert_called_once_with(
                conn._bulk.create_insert_job.return_value,
                "Account",
                iter_mock.return_value,
                120,
                5,
                1,
            )

    def test_bulk_api_update(self):
        sf = Mock()
        sf.bulk_url = "https://salesforce.com"

        conn = Connection(sf)
        conn._bulk = Mock()
        conn._bulk_api_insert_update = Mock(return_value=[])

        with patch("builtins.iter") as iter_mock:
            self.assertEqual([], list(conn.bulk_api_update("Account", [], 120, 5, 1)))

            conn._bulk.create_update_job.assert_called_once_with(
                "Account", contentType="JSON"
            )

            conn._bulk_api_insert_update.assert_called_once_with(
                conn._bulk.create_update_job.return_value,
                "Account",
                iter_mock.return_value,
                120,
                5,
                1,
            )

    def test_bulk_api_insert_update(self):
        sf = Mock()
        sf.bulk_url = "https://salesforce.com"
        conn = Connection(sf)
        conn._bulk = Mock()
        job = Mock()

        retval = [
            [{"Id": "001000000000001"}, {"Id": "001000000000002"}],
            [{"Id": "001000000000003"}],
        ]

        conn._bulk.is_batch_done = Mock(side_effect=[False, True])
        conn._bulk.get_batch_results = Mock(side_effect=retval)

        input_data = [{"Name": "Test"}, {"Name": "Test2"}, {"Name": "Test3"}]
        results = list(
            conn._bulk_api_insert_update(job, "Account", input_data, 120, 5, 2)
        )

        self.assertEqual(2, conn._bulk.post_batch.call_count)
        self.assertEqual(
            conn._bulk.wait_for_batch.call_args_list,
            [
                call(
                    job,
                    conn._bulk.post_batch.return_value,
                    timeout=120,
                    sleep_interval=5,
                ),
                call(
                    job,
                    conn._bulk.post_batch.return_value,
                    timeout=120,
                    sleep_interval=5,
                ),
            ],
        )
        conn._bulk.close_job.assert_called_once_with(job)
        self.assertEqual(
            conn._bulk.get_batch_results.call_args_list,
            [
                call(conn._bulk.post_batch.return_value, job),
                call(conn._bulk.post_batch.return_value, job),
            ],
        )
        self.assertEqual(
            results,
            [
                {"Id": "001000000000001"},
                {"Id": "001000000000002"},
                {"Id": "001000000000003"},
            ],
        )

    def test_retrieve_records_by_id(self):
        id_set = []
        # Generate enough mock Ids to require two queries.
        for i in range(2005):
            new_id = str(amaxa.SalesforceId("00100000000" + str(i + 1).zfill(4)))
            id_set.append(new_id)

        self.assertEqual(2005, len(id_set))

        complete_return_value = [{"Id": each_id} for each_id in id_set]

        api_return_value = [complete_return_value[:2000], complete_return_value[2000:]]
        api_return_value[0].append(None)

        sf = Mock()
        sf.bulk_url = "https://salesforce.com"
        sf.restful = Mock(side_effect=api_return_value)
        conn = Connection(sf)

        retval = conn.retrieve_records_by_id("Account", id_set, ["Name"])
        self.assertEqual(complete_return_value, list(retval))

        self.assertEqual(2, sf.restful.call_count)
        self.assertEqual(
            sf.restful.call_args_list,
            [
                call(
                    "composite/sobjects/Account",
                    method="POST",
                    data=json.dumps({"ids": id_set[:2000], "fields": ["Name"]}),
                ),
                call(
                    "composite/sobjects/Account",
                    method="POST",
                    data=json.dumps({"ids": id_set[2000:], "fields": ["Name"]}),
                ),
            ],
        )

    def test_query_records_by_reference_field(self):
        sf = Mock()
        sf.bulk_url = "https://salesforce.com"
        conn = Connection(sf)

        id_set = []
        for i in range(400):
            new_id = str(amaxa.SalesforceId("00100000000" + str(i + 1).zfill(4)))
            id_set.append(new_id)

        api_return_value = {
            "records": [
                {"Id": "001000000000001", "Name": "test", "Industry": "Finance"}
            ]
        }
        sf.query_all.return_value = api_return_value

        retval = list(
            conn.query_records_by_reference_field(
                "Account", ["Name", "Industry"], "ParentId", id_set
            )
        )

        self.assertGreater(sf.query_all.call_count, 1)
        self.assertEqual(api_return_value["records"] * sf.query_all.call_count, retval)

        # Validate that the WHERE clause length limits were respected
        # and that all of the Ids were queried
        total_ids = 0
        for each_call in sf.query_all.call_args_list:
            argument = each_call[0][0]
            self.assertLessEqual(len(argument[argument.find("WHERE") :]), 4000)
            total_ids += argument.count("'001")

        self.assertEqual(len(id_set), total_ids)
