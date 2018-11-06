import unittest
import amaxa
import os
from simple_salesforce import Salesforce
from unittest.mock import Mock

@unittest.skipIf(any(['INSTANCE_URL' not in os.environ, 'ACCESS_TOKEN' not in os.environ]),
                 'environment not configured for integration test')
class test_Extraction(unittest.TestCase):
    def setUp(self):
        self.connection = Salesforce(
            instance_url=os.environ['INSTANCE_URL'],
            session_id=os.environ['ACCESS_TOKEN']
        )

    def test_all_records_extracts_accounts(self):
        oc = amaxa.OperationContext(self.connection)
        oc.set_output_file('Account', Mock())

        extraction = amaxa.SingleObjectExtraction(
            'Account',
            amaxa.ExtractionScope.ALL_RECORDS,
            ['Id', 'Name']
        )
        oc.add_step(extraction)

        extraction.execute()

        self.assertEqual(5, len(oc.get_extracted_ids('Account')))
    
    def test_query_extracts_self_lookup_hierarchy(self):
        expected_names = {'Caprica Cosmetics', 'Gemenon Gastronomy', 'Aerilon Agrinomics'}
        oc = amaxa.OperationContext(self.connection)
        output = Mock()
        oc.set_output_file('Account', output)

        rec = self.connection.query('SELECT Id FROM Account WHERE Name = \'Caprica Cosmetics\'')
        oc.add_dependency('Account', rec.get('records')[0]['Id'])

        extraction = amaxa.SingleObjectExtraction(
            'Account',
            amaxa.ExtractionScope.SELECTED_RECORDS,
            ['Id', 'Name', 'ParentId']
        )
        oc.add_step(extraction)

        extraction.execute()

        self.assertEqual(3, len(oc.get_extracted_ids('Account')))
        for c in output.call_args_list:
            self.assertIn(c['Name'], expected_names)
            expected_names.remove(c['Name'])
        
        self.assertEqual(0, len(expected_names))
    
    def test_descendents_extracts_object_network(self):
        expected_names = {'Elosha', 'Gaius'}
        oc = amaxa.OperationContext(self.connection)
        output_accounts = Mock()
        output_contacts = Mock()
        oc.set_output_file('Account', output_accounts)
        oc.set_output_file('Contact', output_contacts)

        rec = self.connection.query('SELECT Id FROM Account WHERE Name = \'Caprica Cosmetics\'')
        oc.add_dependency('Account', rec.get('records')[0]['Id'])

        oc.add_step(
            amaxa.SingleObjectExtraction(
                'Account',
                amaxa.ExtractionScope.SELECTED_RECORDS,
                ['Id', 'Name', 'ParentId']
            )
        )
        oc.add_step(
            amaxa.SingleObjectExtraction(
                'Contact',
                amaxa.ExtractionScope.DESCENDENTS,
                ['Id', 'FirstName', 'LastName', 'AccountId']
            )
        )

        oc.execute()

        self.assertEqual(3, len(oc.get_extracted_ids('Account')))
        self.assertEqual(2, len(oc.get_extracted_ids('Contact')))
        for c in output_contacts.call_args_list:
            self.assertIn(c['FirstName'], expected_names)
            expected_names.remove(c['FirstName'])

        self.assertEqual(0, len(expected_names))

if __name__ == "__main__":
    unittest.main()