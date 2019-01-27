import unittest
import os
import io
import csv
from simple_salesforce import Salesforce
from unittest.mock import Mock
from .. import amaxa
from .. import loader
from ..__main__ import main as main

@unittest.skipIf(any(['INSTANCE_URL' not in os.environ, 'ACCESS_TOKEN' not in os.environ]),
                 'environment not configured for integration test')
class test_integration_high_volume(unittest.TestCase):
    def setUp(self):
        self.connection = Salesforce(
            instance_url=os.environ['INSTANCE_URL'],
            session_id=os.environ['ACCESS_TOKEN']
        )

    def tearDown(self):
        # We have to run this twice to stick within the DML Rows limit.
        self.connection.restful(
            'tooling/executeAnonymous', 
            {
                'anonymousBody': 'delete [SELECT Id FROM Lead LIMIT 50000];'
            }
        )
        self.connection.restful(
            'tooling/executeAnonymous', 
            {
                'anonymousBody': 'delete [SELECT Id FROM Lead LIMIT 50000];'
            }
        )

    def test_loads_and_extracts_high_data_volume(self):
        # This is a single unit test rather than multiple to save on execution time.
        records = 'Id,Company,LastName\n'

        for i in range(100000):
            records += '00Q000000{:06d},[not provided],Lead {:06d}\n'.format(i, i)

        op = amaxa.LoadOperation(self.connection)
        op.set_input_file('Lead', csv.DictReader(io.StringIO(records)))
        op.add_step(amaxa.LoadStep('Lead', set(['LastName', 'Company'])))

        op.execute()

        self.assertEqual(
            100000,
            self.connection.query('SELECT count() FROM Lead').get('totalSize')
        )

        oc = amaxa.ExtractOperation(self.connection)
        oc.set_output_file('Lead', Mock(), Mock())

        extraction = amaxa.ExtractionStep(
            'Lead',
            amaxa.ExtractionScope.ALL_RECORDS,
            ['Id', 'LastName']
        )
        oc.add_step(extraction)

        extraction.execute()

        self.assertEqual(100000, len(oc.get_extracted_ids('Lead')))