import os
import unittest

from simple_salesforce import Salesforce
from simple_salesforce.exceptions import (
    SalesforceMalformedRequest,
    SalesforceResourceNotFound,
)


@unittest.skipIf(
    any(["INSTANCE_URL" not in os.environ, "ACCESS_TOKEN" not in os.environ]),
    "environment not configured for integration test",
)
class IntegrationTest(unittest.TestCase):
    session_records = {}
    connection = None

    @classmethod
    def setUpClass(cls):
        cls.connection = Salesforce(
            instance_url=os.environ["INSTANCE_URL"],
            session_id=os.environ["ACCESS_TOKEN"],
            version="48.0",
        )

    @classmethod
    def tearDownClass(cls):
        cls._delete_stored_records(cls.session_records)

    @classmethod
    def register_session_record(cls, obj_type, record_id):
        cls.session_records.setdefault(obj_type, []).append(record_id)

    @classmethod
    def _delete_sobject_records(cls, sobject, records):
        proxy = getattr(cls.connection, sobject)

        for record_id in records:
            try:
                proxy.delete(record_id)
            except (SalesforceResourceNotFound, SalesforceMalformedRequest):
                pass  # Ignore exceptions caused by cascade deletes.

    @classmethod
    def _delete_stored_records(cls, record_set):
        must_go_first = ["Opportunity", "Case"]

        for sobject in [s for s in record_set if s in must_go_first]:
            cls._delete_sobject_records(sobject, record_set[sobject])
        for sobject in [s for s in record_set if s not in must_go_first]:
            cls._delete_sobject_records(sobject, record_set[sobject])

    def setUp(self):
        self.case_records = {}

    def tearDown(self):
        self._delete_stored_records(self.case_records)

    def register_case_record(self, obj_type, record_id):
        self.case_records.setdefault(obj_type, []).append(record_id)
