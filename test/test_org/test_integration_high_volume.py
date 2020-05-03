import os
import unittest
from test.test_unit.MockFileStore import MockFileStore

from simple_salesforce import Salesforce

import amaxa
from amaxa import constants
from amaxa.api import Connection


@unittest.skipIf(
    any(["INSTANCE_URL" not in os.environ, "ACCESS_TOKEN" not in os.environ]),
    "environment not configured for integration test",
)
class test_integration_high_volume(unittest.TestCase):
    def setUp(self):
        self.connection = Salesforce(
            instance_url=os.environ["INSTANCE_URL"],
            session_id=os.environ["ACCESS_TOKEN"],
            version=constants.OPTION_DEFAULTS["api-version"],
        )

    def tearDown(self):
        # We have to run this repeatedly to stick within the DML Rows limit.
        for i in range(10):
            self.connection.restful(
                "tooling/executeAnonymous",
                {"anonymousBody": "delete [SELECT Id FROM Lead LIMIT 10000];"},
            )

    def test_loads_and_extracts_high_data_volume(self):
        # This is a single unit test rather than multiple to save on execution time.
        records = []

        for i in range(100000):
            records.append(
                {
                    "Id": "00Q000000{:06d}".format(i),
                    "Company": "[not provided]",
                    "LastName": "Lead {:06d}".format(i),
                }
            )

        op = amaxa.LoadOperation(Connection(self.connection, "48.0"))
        op.file_store = MockFileStore()
        op.file_store.records["Lead"] = records
        op.add_step(amaxa.LoadStep("Lead", set(["LastName", "Company"])))

        op.initialize()
        op.execute()

        self.assertEqual(
            100000, self.connection.query("SELECT count() FROM Lead").get("totalSize")
        )

        oc = amaxa.ExtractOperation(Connection(self.connection, "48.0"))
        oc.file_store = MockFileStore()

        extraction = amaxa.ExtractionStep(
            "Lead", amaxa.ExtractionScope.ALL_RECORDS, ["Id", "LastName"]
        )
        oc.add_step(extraction)

        extraction.initialize()
        extraction.execute()

        self.assertEqual(100000, len(oc.get_extracted_ids("Lead")))
