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
        oc.set_output_file('Account', Mock(), Mock())

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
        oc.set_output_file('Account', output, Mock())

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
        oc.set_output_file('Account', output_accounts, Mock())
        oc.set_output_file('Contact', output_contacts, Mock())

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
        oc.set_output_file('Account', output_accounts, Mock())
        oc.set_output_file('Contact', output_contacts, Mock())

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
        oc.set_output_file('Account', output_accounts, Mock())
        oc.set_output_file('User', output_users, Mock())

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


@unittest.skipIf(any(['INSTANCE_URL' not in os.environ, 'ACCESS_TOKEN' not in os.environ]),
                 'environment not configured for integration test')
class test_Integration_Load(unittest.TestCase):
    def setUp(self):
        self.connection = Salesforce(
            instance_url=os.environ['INSTANCE_URL'],
            session_id=os.environ['ACCESS_TOKEN']
        )

    def tearDown(self):
        self.connection.restful(
            'tooling/executeAnonymous', 
            {
                'anonymousBody': 'delete [SELECT Id FROM Lead]; delete [SELECT Id FROM Product2]; delete [SELECT Id FROM Campaign];'
            }
        )

    def test_loads_single_object(self):
        # To avoid conflict, we load an object (Product2) not used in other load or extract tests.
        records = '''
Id,Name,IsActive,ProductCode
01t000000000001,Tauron Taffy,true,TAFFY_TAUR
01t000000000002,Gemenese Goulash,true,GLSH
01t000000000003AAA,CapricaCorn,false,CPRCC
        '''.strip()

        op = amaxa.LoadOperation(self.connection)
        op.set_input_file('Product2', csv.DictReader(io.StringIO(records)))
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
        # Campaign has a self-lookup, ParentId
        campaigns = '''
Id,Name,IsActive,ParentId
701000000000001,Tauron Tourist Outreach,true,
701000000000002,Aerilon Outreach,true,701000000000001
701000000000003AAA,Caprica City Direct Mailer,false,701000000000001
        '''.strip()
        leads = '''
Id,Company,LastName
00Q000000000001,Picon Fleet Headquarters,Nagata
00Q000000000002,Picon Fleet Headquarters,Adama
00Q000000000003,Ha-La-Tha,Guatrau
00Q000000000004,[not provided],Thrace
        '''.strip()
        campaign_members='''
Id,CampaignId,LeadId,Status
00v000000000001,701000000000001,00Q000000000001,Sent
00v000000000002,701000000000002,00Q000000000002,Sent
00v000000000003,701000000000003,00Q000000000004,Sent
00v000000000004,701000000000001,00Q000000000004,Sent
        '''.strip()

        op = amaxa.LoadOperation(self.connection)
        op.set_input_file('Campaign', csv.DictReader(io.StringIO(campaigns)))
        op.set_input_file('Lead', csv.DictReader(io.StringIO(leads)))
        op.set_input_file('CampaignMember', csv.DictReader(io.StringIO(campaign_members)))

        op.add_step(amaxa.LoadStep('Campaign', set(['Name', 'IsActive', 'ParentId'])))
        op.add_step(amaxa.LoadStep('Lead', set(['Company', 'LastName'])))
        op.add_step(amaxa.LoadStep('CampaignMember', set(['CampaignId', 'LeadId', 'Status'])))

        op.execute()

        loaded_campaigns = self.connection.query_all('SELECT Name, IsActive, (SELECT Name FROM ChildCampaigns) FROM Campaign').get('records')
        self.assertEqual(3, len(loaded_campaigns))
        required_names = { 'Tauron Tourist Outreach', 'Aerilon Outreach', 'Caprica City Direct Mailer' }
        for r in loaded_campaigns:
            self.assertIn(r['Name'], required_names)
            required_names.remove(r['Name'])
            if r['Name'] == 'Tauron Tourist Outreach':
                self.assertEqual(2, len(r['ChildCampaigns']['records']))

        self.assertEqual(0, len(required_names))

        loaded_leads = self.connection.query_all('SELECT LastName, Company, (SELECT Name FROM CampaignMembers) FROM Lead').get('records')
        self.assertEqual(4, len(loaded_leads))
        required_names = { 'Nagata', 'Adama', 'Guatrau', 'Thrace' }
        for r in loaded_leads:
            self.assertIn(r['LastName'], required_names)
            required_names.remove(r['LastName'])
            if r['LastName'] == 'Nagata':
                self.assertEqual(1, len(r['CampaignMembers']['records']))
            elif r['LastName'] == 'Thrace':
                self.assertEqual(2, len(r['CampaignMembers']['records']))
            if r['LastName'] == 'Adama':
                self.assertEqual(1, len(r['CampaignMembers']['records']))

        self.assertEqual(0, len(required_names))

        loaded_campaign_members = self.connection.query_all('SELECT Id FROM CampaignMember').get('records')
        self.assertEqual(4, len(loaded_campaign_members))


if __name__ == "__main__":
    unittest.main()