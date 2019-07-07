import unittest
import os
from simple_salesforce import Salesforce
from .. import amaxa
from ..api import Connection
from .MockFileStore import MockFileStore


@unittest.skipIf(
    any(["INSTANCE_URL" not in os.environ, "ACCESS_TOKEN" not in os.environ]),
    "environment not configured for integration test",
)
class test_Integration_Extraction(unittest.TestCase):
    def setUp(self):
        self.connection = Salesforce(
            instance_url=os.environ["INSTANCE_URL"],
            session_id=os.environ["ACCESS_TOKEN"],
            version="46.0",
        )

    def test_all_records_extracts_accounts(self):
        oc = amaxa.ExtractOperation(Connection(self.connection))
        oc.file_store = MockFileStore()

        extraction = amaxa.ExtractionStep(
            "Account", amaxa.ExtractionScope.ALL_RECORDS, ["Id", "Name"]
        )
        oc.add_step(extraction)

        extraction.initialize()
        extraction.execute()

        self.assertEqual(5, len(oc.get_extracted_ids("Account")))

    def test_query_extracts_self_lookup_hierarchy(self):
        expected_names = {
            "Caprica Cosmetics",
            "Gemenon Gastronomy",
            "Aerilon Agrinomics",
        }
        oc = amaxa.ExtractOperation(Connection(self.connection))
        oc.file_store = MockFileStore()

        rec = self.connection.query(
            "SELECT Id FROM Account WHERE Name = 'Caprica Cosmetics'"
        )
        oc.add_dependency("Account", rec.get("records")[0]["Id"])

        extraction = amaxa.ExtractionStep(
            "Account",
            amaxa.ExtractionScope.SELECTED_RECORDS,
            ["Id", "Name", "ParentId"],
        )
        oc.add_step(extraction)

        extraction.initialize()
        extraction.execute()

        self.assertEqual(3, len(oc.get_extracted_ids("Account")))
        for c in oc.file_store.get_csv(
            "Account", amaxa.FileType.OUTPUT
        ).writerow.call_args_list:
            self.assertIn(c[0][0]["Name"], expected_names)
            expected_names.remove(c[0][0]["Name"])

        self.assertEqual(0, len(expected_names))

    def test_descendents_extracts_object_network(self):
        expected_names = {"Elosha", "Gaius"}
        oc = amaxa.ExtractOperation(Connection(self.connection))
        oc.file_store = MockFileStore()

        rec = self.connection.query(
            "SELECT Id FROM Account WHERE Name = 'Caprica Cosmetics'"
        )
        oc.add_dependency("Account", rec.get("records")[0]["Id"])

        oc.add_step(
            amaxa.ExtractionStep(
                "Account",
                amaxa.ExtractionScope.SELECTED_RECORDS,
                ["Id", "Name", "ParentId"],
            )
        )
        oc.add_step(
            amaxa.ExtractionStep(
                "Contact",
                amaxa.ExtractionScope.DESCENDENTS,
                ["Id", "FirstName", "LastName", "AccountId"],
            )
        )

        oc.initialize()
        oc.execute()

        self.assertEqual(3, len(oc.get_extracted_ids("Account")))
        self.assertEqual(2, len(oc.get_extracted_ids("Contact")))
        for c in oc.file_store.get_csv(
            "Contact", amaxa.FileType.OUTPUT
        ).writerow.call_args_list:
            self.assertIn(c[0][0]["FirstName"], expected_names)
            expected_names.remove(c[0][0]["FirstName"])

        self.assertEqual(0, len(expected_names))

    def test_extracts_dependencies(self):
        expected_account_names = {
            "Caprica Cosmetics",
            "Gemenon Gastronomy",
            "Aerilon Agrinomics",
        }
        expected_contact_names = {"Gaius"}

        oc = amaxa.ExtractOperation(Connection(self.connection))
        oc.file_store = MockFileStore()

        rec = self.connection.query("SELECT Id FROM Contact WHERE LastName = 'Baltar'")
        oc.add_dependency("Contact", rec.get("records")[0]["Id"])

        oc.add_step(
            amaxa.ExtractionStep(
                "Contact",
                amaxa.ExtractionScope.SELECTED_RECORDS,
                ["Id", "FirstName", "LastName", "AccountId"],
            )
        )
        oc.add_step(
            amaxa.ExtractionStep(
                "Account", amaxa.ExtractionScope.DESCENDENTS, ["Id", "Name", "ParentId"]
            )
        )

        oc.initialize()
        oc.execute()

        self.assertEqual(3, len(oc.get_extracted_ids("Account")))
        self.assertEqual(1, len(oc.get_extracted_ids("Contact")))

        for c in oc.file_store.get_csv(
            "Contact", amaxa.FileType.OUTPUT
        ).writerow.call_args_list:
            self.assertIn(c[0][0]["FirstName"], expected_contact_names)
            expected_contact_names.remove(c[0][0]["FirstName"])
        self.assertEqual(0, len(expected_contact_names))

        for c in oc.file_store.get_csv(
            "Account", amaxa.FileType.OUTPUT
        ).writerow.call_args_list:
            self.assertIn(c[0][0]["Name"], expected_account_names)
            expected_account_names.remove(c[0][0]["Name"])
        self.assertEqual(0, len(expected_account_names))

    def test_extracts_polymorphic_lookups(self):
        oc = amaxa.ExtractOperation(Connection(self.connection))
        oc.file_store = MockFileStore()

        rec = self.connection.query(
            "SELECT Id FROM Account WHERE Name = 'Caprica Cosmetics'"
        )
        oc.add_dependency("Account", rec.get("records")[0]["Id"])

        oc.add_step(
            amaxa.ExtractionStep(
                "Account",
                amaxa.ExtractionScope.SELECTED_RECORDS,
                ["Id", "Name", "OwnerId"],
            )
        )
        oc.add_step(
            amaxa.ExtractionStep(
                "User", amaxa.ExtractionScope.DESCENDENTS, ["Id", "Username"]
            )
        )

        oc.initialize()
        oc.execute()

        self.assertEqual(1, len(oc.get_extracted_ids("Account")))
        self.assertEqual(1, len(oc.get_extracted_ids("User")))


@unittest.skipIf(
    any(["INSTANCE_URL" not in os.environ, "ACCESS_TOKEN" not in os.environ]),
    "environment not configured for integration test",
)
class test_Integration_Load(unittest.TestCase):
    def setUp(self):
        self.connection = Salesforce(
            instance_url=os.environ["INSTANCE_URL"],
            session_id=os.environ["ACCESS_TOKEN"],
            version="46.0",
        )

    def tearDown(self):
        self.connection.restful(
            "tooling/executeAnonymous",
            {
                "anonymousBody": "delete [SELECT Id FROM Lead]; "
                "delete [SELECT Id FROM Product2]; "
                "delete [SELECT Id FROM Campaign];"
            },
        )

    def test_loads_single_object(self):
        # To avoid conflict, we load an object (Product2) not used in other load
        # or extract tests.
        records = [
            {
                "Id": "01t000000000001",
                "Name": "Tauron Taffy",
                "IsActive": "True",
                "ProductCode": "TAFFY_TAUR",
            },
            {
                "Id": "01t000000000002",
                "Name": "Gemenese Goulash",
                "IsActive": "True",
                "ProductCode": "GLSH",
            },
            {
                "Id": "01t000000000003AAA",
                "Name": "CapricaCorn",
                "IsActive": "False",
                "ProductCode": "CPRCC",
            },
        ]

        op = amaxa.LoadOperation(Connection(self.connection))
        op.file_store = MockFileStore()
        op.file_store.records["Product2"] = records

        op.add_step(
            amaxa.LoadStep(
                "Product2", set(["Name", "IsActive", "ProductCode", "Description"])
            )
        )

        op.initialize()
        op.execute()

        loaded_products = self.connection.query_all(
            "SELECT Name, IsActive, ProductCode FROM Product2"
        ).get("records")
        self.assertEqual(3, len(loaded_products))
        required_names = {x["Name"] for x in records}
        for r in loaded_products:
            self.assertIn(r["Name"], required_names)
            required_names.remove(r["Name"])

        self.assertEqual(0, len(required_names))

    def test_loads_complex_hierarchy(self):
        # To avoid conflict with other load tests and with extract tests,
        # we load Campaigns, Campaign Members, and Leads.
        # Campaign has a self-lookup, ParentId

        campaigns = [
            {
                "Id": "701000000000001",
                "Name": "Tauron Tourist Outreach",
                "IsActive": "True",
                "ParentId": "",
            },
            {
                "Id": "701000000000002",
                "Name": "Aerilon Outreach",
                "IsActive": "True",
                "ParentId": "701000000000001",
            },
            {
                "Id": "701000000000003AAA",
                "Name": "Caprica City Direct Mailer",
                "IsActive": "False",
                "ParentId": "701000000000001",
            },
        ]
        leads = [
            {
                "Id": "00Q000000000001",
                "Company": "Picon Fleet Headquarters",
                "LastName": "Nagata",
            },
            {
                "Id": "00Q000000000002",
                "Company": "Picon Fleet Headquarters",
                "LastName": "Adama",
            },
            {"Id": "00Q000000000003", "Company": "Ha-La-Tha", "LastName": "Guatrau"},
            {
                "Id": "00Q000000000004",
                "Company": "[not provided]",
                "LastName": "Thrace",
            },
        ]
        campaign_members = [
            {
                "Id": "00v000000000001",
                "CampaignId": "701000000000001",
                "LeadId": "00Q000000000001",
                "Status": "Sent",
            },
            {
                "Id": "00v000000000002",
                "CampaignId": "701000000000002",
                "LeadId": "00Q000000000002",
                "Status": "Sent",
            },
            {
                "Id": "00v000000000003",
                "CampaignId": "701000000000003",
                "LeadId": "00Q000000000004",
                "Status": "Sent",
            },
            {
                "Id": "00v000000000004",
                "CampaignId": "701000000000001",
                "LeadId": "00Q000000000004",
                "Status": "Sent",
            },
        ]

        op = amaxa.LoadOperation(Connection(self.connection))
        op.file_store = MockFileStore()
        op.file_store.records["Campaign"] = campaigns
        op.file_store.records["Lead"] = leads
        op.file_store.records["CampaignMember"] = campaign_members

        op.add_step(amaxa.LoadStep("Campaign", set(["Name", "IsActive", "ParentId"])))
        op.add_step(amaxa.LoadStep("Lead", set(["Company", "LastName"])))
        op.add_step(
            amaxa.LoadStep("CampaignMember", set(["CampaignId", "LeadId", "Status"]))
        )

        op.initialize()
        op.execute()

        loaded_campaigns = self.connection.query_all(
            "SELECT Name, IsActive, (SELECT Name FROM ChildCampaigns) FROM Campaign"
        ).get("records")
        self.assertEqual(3, len(loaded_campaigns))
        required_names = {x["Name"] for x in campaigns}
        for r in loaded_campaigns:
            self.assertIn(r["Name"], required_names)
            required_names.remove(r["Name"])
            if r["Name"] == "Tauron Tourist Outreach":
                self.assertEqual(2, len(r["ChildCampaigns"]["records"]))

        self.assertEqual(0, len(required_names))

        loaded_leads = self.connection.query_all(
            "SELECT LastName, Company, (SELECT Name FROM CampaignMembers) FROM Lead"
        ).get("records")
        self.assertEqual(4, len(loaded_leads))
        required_names = {x["LastName"] for x in leads}
        for r in loaded_leads:
            self.assertIn(r["LastName"], required_names)
            required_names.remove(r["LastName"])
            if r["LastName"] == "Nagata":
                self.assertEqual(1, len(r["CampaignMembers"]["records"]))
            elif r["LastName"] == "Thrace":
                self.assertEqual(2, len(r["CampaignMembers"]["records"]))
            if r["LastName"] == "Adama":
                self.assertEqual(1, len(r["CampaignMembers"]["records"]))

        self.assertEqual(0, len(required_names))

        loaded_campaign_members = self.connection.query_all(
            "SELECT Id FROM CampaignMember"
        ).get("records")
        self.assertEqual(4, len(loaded_campaign_members))
