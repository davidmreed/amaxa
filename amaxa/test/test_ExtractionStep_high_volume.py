import unittest
from unittest.mock import Mock
from .MockConnection import MockConnection
from .. import amaxa


class test_ExtractionStep_high_volume(unittest.TestCase):
    def test_perform_bulk_api_pass_stores_high_volume_results(self):
        retval = []
        for i in range(500000):
            retval.append(
                {"Id": "001000000{:06d}".format(i), "Name": "Account {:06d}".format(i)}
            )

        connection = MockConnection(bulk_query_results=retval)
        oc = amaxa.ExtractOperation(Mock(wraps=connection))

        step = amaxa.ExtractionStep(
            "Account", amaxa.ExtractionScope.ALL_RECORDS, ["Name"]
        )
        step.store_result = Mock()
        oc.add_step(step)
        step.initialize()

        step.perform_bulk_api_pass("SELECT Id FROM Account")
        self.assertEqual(500000, step.store_result.call_count)
