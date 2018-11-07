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
        for c in output.writerow.call_args_list:
            self.assertIn(c[0][0]['Name'], expected_names)
            expected_names.remove(c[0][0]['Name'])
        
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
        for c in output_contacts.writerow.call_args_list:
            self.assertIn(c[0][0]['FirstName'], expected_names)
            expected_names.remove(c[0][0]['FirstName'])

        self.assertEqual(0, len(expected_names))

    def test_extracts_dependencies(self):
        expected_account_names = {'Caprica Cosmetics', 'Gemenon Gastronomy', 'Aerilon Agrinomics'}
        expected_contact_names = {'Gaius'}

        oc = amaxa.OperationContext(self.connection)
        output_accounts = Mock()
        output_contacts = Mock()
        oc.set_output_file('Account', output_accounts)
        oc.set_output_file('Contact', output_contacts)

        rec = self.connection.query('SELECT Id FROM Contact WHERE LastName = \'Baltar\'')
        oc.add_dependency('Contact', rec.get('records')[0]['Id'])

        oc.add_step(
            amaxa.SingleObjectExtraction(
                'Contact',
                amaxa.ExtractionScope.SELECTED_RECORDS,
                ['Id', 'FirstName', 'LastName', 'AccountId']
            )
        )
        oc.add_step(
            amaxa.SingleObjectExtraction(
                'Account',
                amaxa.ExtractionScope.DESCENDENTS,
                ['Id', 'Name', 'ParentId']
            )
        )

        oc.execute()

        self.assertEqual(3, len(oc.get_extracted_ids('Account')))
        self.assertEqual(1, len(oc.get_extracted_ids('Contact')))

        for c in output_contacts.writerow.call_args_list:
            self.assertIn(c[0][0]['FirstName'], expected_contact_names)
            expected_contact_names.remove(c[0][0]['FirstName'])
        self.assertEqual(0, len(expected_contact_names))

        self.assertEqual(3, len(oc.get_extracted_ids('Account')))
        print(output_accounts.writerow.call_args_list)
        for c in output_accounts.writerow.call_args_list:
            self.assertIn(c[0][0]['Name'], expected_account_names)
            expected_account_names.remove(c[0][0]['Name'])
        
        self.assertEqual(0, len(expected_account_names))

    def test_extracts_polymorphic_lookups(self):
        oc = amaxa.OperationContext(self.connection)
        output_accounts = Mock()
        output_users = Mock()
        oc.set_output_file('Account', output_accounts)
        oc.set_output_file('User', output_users)

        rec = self.connection.query('SELECT Id FROM Account WHERE Name = \'Caprica Cosmetics\'')
        oc.add_dependency('Account', rec.get('records')[0]['Id'])

        oc.add_step(
            amaxa.SingleObjectExtraction(
                'Account',
                amaxa.ExtractionScope.SELECTED_RECORDS,
                ['Id', 'Name', 'OwnerId']
            )
        )
        oc.add_step(
            amaxa.SingleObjectExtraction(
                'User',
                amaxa.ExtractionScope.DESCENDENTS,
                ['Id', 'Username']
            )
        )

        oc.execute()

        self.assertEqual(1, len(oc.get_extracted_ids('Account')))
        self.assertEqual(1, len(oc.get_extracted_ids('User')))

    def test_extracts_from_command_line(self):
        pass

if __name__ == "__main__":
    unittest.main()