import unittest
import os
import io
import csv
from simple_salesforce import Salesforce
from unittest.mock import Mock
from . import amaxa
from . import loader
from .__main__ import main as main

@unittest.skipIf(any(['INSTANCE_URL' not in os.environ, 'ACCESS_TOKEN' not in os.environ]),
                 'environment not configured for integration test')
class test_Integration_Extraction(unittest.TestCase):
    def setUp(self):
        self.connection = Salesforce(
            instance_url=os.environ['INSTANCE_URL'],
            session_id=os.environ['ACCESS_TOKEN']
        )

    def test_all_records_extracts_accounts(self):
        oc = amaxa.ExtractOperation(self.connection)
        oc.set_output_file('Account', Mock())

        extraction = amaxa.ExtractionStep(
            'Account',
            amaxa.ExtractionScope.ALL_RECORDS,
            ['Id', 'Name']
        )
        oc.add_step(extraction)

        extraction.execute()

        self.assertEqual(5, len(oc.get_extracted_ids('Account')))
    
    def test_query_extracts_self_lookup_hierarchy(self):
        expected_names = {'Caprica Cosmetics', 'Gemenon Gastronomy', 'Aerilon Agrinomics'}
        oc = amaxa.ExtractOperation(self.connection)
        output = Mock()
        oc.set_output_file('Account', output)

        rec = self.connection.query('SELECT Id FROM Account WHERE Name = \'Caprica Cosmetics\'')
        oc.add_dependency('Account', rec.get('records')[0]['Id'])

        extraction = amaxa.ExtractionStep(
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
        oc = amaxa.ExtractOperation(self.connection)
        output_accounts = Mock()
        output_contacts = Mock()
        oc.set_output_file('Account', output_accounts)
        oc.set_output_file('Contact', output_contacts)

        rec = self.connection.query('SELECT Id FROM Account WHERE Name = \'Caprica Cosmetics\'')
        oc.add_dependency('Account', rec.get('records')[0]['Id'])

        oc.add_step(
            amaxa.ExtractionStep(
                'Account',
                amaxa.ExtractionScope.SELECTED_RECORDS,
                ['Id', 'Name', 'ParentId']
            )
        )
        oc.add_step(
            amaxa.ExtractionStep(
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

        oc = amaxa.ExtractOperation(self.connection)
        output_accounts = Mock()
        output_contacts = Mock()
        oc.set_output_file('Account', output_accounts)
        oc.set_output_file('Contact', output_contacts)

        rec = self.connection.query('SELECT Id FROM Contact WHERE LastName = \'Baltar\'')
        oc.add_dependency('Contact', rec.get('records')[0]['Id'])

        oc.add_step(
            amaxa.ExtractionStep(
                'Contact',
                amaxa.ExtractionScope.SELECTED_RECORDS,
                ['Id', 'FirstName', 'LastName', 'AccountId']
            )
        )
        oc.add_step(
            amaxa.ExtractionStep(
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
        oc = amaxa.ExtractOperation(self.connection)
        output_accounts = Mock()
        output_users = Mock()
        oc.set_output_file('Account', output_accounts)
        oc.set_output_file('User', output_users)

        rec = self.connection.query('SELECT Id FROM Account WHERE Name = \'Caprica Cosmetics\'')
        oc.add_dependency('Account', rec.get('records')[0]['Id'])

        oc.add_step(
            amaxa.ExtractionStep(
                'Account',
                amaxa.ExtractionScope.SELECTED_RECORDS,
                ['Id', 'Name', 'OwnerId']
            )
        )
        oc.add_step(
            amaxa.ExtractionStep(
                'User',
                amaxa.ExtractionScope.DESCENDENTS,
                ['Id', 'Username']
            )
        )

        oc.execute()

        self.assertEqual(1, len(oc.get_extracted_ids('Account')))
        self.assertEqual(1, len(oc.get_extracted_ids('User')))

    def test_extracts_from_command_line(self):
        contact_mock = io.StringIO()
        account_mock = io.StringIO()

        expected_account_names = {'Picon Fleet Headquarters'}
        expected_contact_names = {'Admiral'}

        def select_file(f, *args, **kwargs):
            credentials = '''
                version: 1
                credentials:
                    access-token: '{}'
                    instance-url: '{}'
                '''.format(os.environ['ACCESS_TOKEN'], os.environ['INSTANCE_URL'])

            extraction = '''
                version: 1
                operation:
                    - 
                        sobject: Account
                        fields: 
                            - Name
                            - Id
                            - ParentId
                        extract: 
                            query: "Name = 'Picon Fleet Headquarters'"
                    -
                        sobject: Contact
                        fields:
                            - FirstName
                            - LastName
                            - AccountId
                        extract:
                            descendents: True
                '''
            m = None
            if f == 'credentials.yaml':
                m = unittest.mock.mock_open(read_data=credentials)(f, *args, **kwargs)
                m.name = f
            elif f == 'extraction.yaml':
                m = unittest.mock.mock_open(read_data=extraction)(f, *args, **kwargs)
                m.name = f
            elif f == 'Account.csv':
                m = account_mock
            elif f == 'Contact.csv':
                m = contact_mock
            
            return m

        m = Mock(side_effect=select_file)
        with unittest.mock.patch('builtins.open', m):
            with unittest.mock.patch(
                'sys.argv',
                ['amaxa', '-c', 'credentials.yaml', 'extraction.yaml']
            ):
                return_value = main()

                self.assertEqual(0, return_value)

        account_mock.seek(0)
        account_reader = csv.DictReader(account_mock)
        for row in account_reader:
            self.assertIn(row['Name'], expected_account_names)
            expected_account_names.remove(row['Name'])
        self.assertEqual(0, len(expected_account_names))
        self.assertEqual(set(['Id', 'Name', 'ParentId']), set(account_reader.fieldnames))

        contact_mock.seek(0)
        contact_reader = csv.DictReader(contact_mock)
        for row in contact_reader:
            self.assertIn(row['FirstName'], expected_contact_names)
            expected_contact_names.remove(row['FirstName'])
        self.assertEqual(0, len(expected_contact_names))
        self.assertEqual(set(['FirstName', 'LastName', 'AccountId', 'Id']), set(contact_reader.fieldnames))

@unittest.skipIf(any(['INSTANCE_URL' not in os.environ, 'ACCESS_TOKEN' not in os.environ]),
                 'environment not configured for integration test')
class test_Integration_Load(unittest.TestCase):
    def setUp(self):
        self.connection = Salesforce(
            instance_url=os.environ['INSTANCE_URL'],
            session_id=os.environ['ACCESS_TOKEN']
        )

    def test_loads_single_object(self):
        # To avoid conflict, we load an object (Product2) not used in other load or extract tests.
        records = '''
        Id,Name,IsActive,ProductCode
        01t000000000001,Tauron Taffy,true,TAFFY_TAUR
        01t0000000000002,Gemenese Goulash,true,GLSH
        01t0000000000003AAA,CapricaCorn,false,CPRCC
        '''

        op = amaxa.LoadOperation(self.connection)
        op.set_input_file('Product2', csv.DictReader(records))
        op.add_step(amaxa.LoadStep('Product2', set(['Name', 'IsActive', 'ProductCode', 'Description'])))

        op.execute()

        loaded_products = self.connection.query_all('SELECT Name, IsActive, ProductCode FROM Product2').get('records')
        self.assertEqual(3, len(loaded_products))
        required_names = { 'Tauron Taffy', 'Gemenese Goulash', 'CapricaCorn' }
        for r in loaded_products:
            self.assertIn(r['Name'], required_names)
            required_names.remove(r['Name'])
        
        self.assertEqual(0, len(required_names))

    def test_loads_complex_hierarchy(self):
        # To avoid conflict with other load tests and with extract tests,
        # we load Campaigns, Campaign Members, and Leads.
        pass
    
    def test_loads_from_command_line(self):
        pass

if __name__ == "__main__":
    unittest.main()