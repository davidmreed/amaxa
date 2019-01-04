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
                 'environment not configured for end-to-end test')
class test_end_to_end(unittest.TestCase):
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

    def test_extracts_from_command_line(self):
        contact_mock = io.StringIO()
        account_mock = io.StringIO()
        account_mock.close = Mock()
        contact_mock.close = Mock()

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

    def test_loads_from_command_line(self):
        # To avoid conflict with extract tests, we load Campaigns, Campaign Members, and Leads.
        campaigns = io.StringIO('''
Id,Name,IsActive,ParentId
701000000000001,Tauron Tourist Outreach,true,
701000000000002,Aerilon Outreach,true,701000000000001
701000000000003AAA,Caprica City Direct Mailer,false,701000000000001
        '''.strip())
        leads = io.StringIO('''
Id,Company,LastName
00Q000000000001,Picon Fleet Headquarters,Nagata
00Q000000000002,Picon Fleet Headquarters,Adama
00Q000000000003,Ha-La-Tha,Guatrau
00Q000000000004,[not provided],Thrace
        '''.strip())
        campaign_members = io.StringIO('''
Id,CampaignId,LeadId,Status
00v000000000001,701000000000001,00Q000000000001,Sent
00v000000000002,701000000000002,00Q000000000002,Sent
00v000000000003,701000000000003,00Q000000000004,Sent
00v000000000004,701000000000001,00Q000000000004,Sent
        '''.strip())

        def select_file(f, *args, **kwargs):
            credentials = '''
                version: 1
                credentials:
                    access-token: '{}'
                    instance-url: '{}'
                '''.format(os.environ['ACCESS_TOKEN'], os.environ['INSTANCE_URL'])

            load = '''
                version: 1
                operation:
                    - 
                        sobject: Campaign
                        fields: 
                            - Name
                            - ParentId
                            - IsActive
                    -
                        sobject: Lead
                        fields:
                            - LastName
                            - Company
                    -
                        sobject: CampaignMember
                        fields:
                            - LeadId
                            - CampaignId
                            - Status
                '''
            m = None
            if f == 'credentials.yaml':
                m = unittest.mock.mock_open(read_data=credentials)(f, *args, **kwargs)
                m.name = f
            elif f == 'load.yaml':
                m = unittest.mock.mock_open(read_data=load)(f, *args, **kwargs)
                m.name = f
            elif f == 'Campaign.csv':
                m = campaigns
            elif f == 'Lead.csv':
                m = leads
            elif f == 'CampaignMember.csv':
                m = campaign_members
            else:
                m = unittest.mock.mock_open()(f, *args, **kwargs)
            
            return m

        m = Mock(side_effect=select_file)
        with unittest.mock.patch('builtins.open', m):
            with unittest.mock.patch(
                'sys.argv',
                ['amaxa', '-c', 'credentials.yaml', '--load', 'load.yaml']
            ):
                return_value = main()

                self.assertEqual(0, return_value)

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
