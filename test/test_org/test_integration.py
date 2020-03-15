from test.test_org.IntegrationTest import IntegrationTest
from test.test_unit.MockFileStore import MockFileStore

import amaxa
from amaxa.api import Connection


class test_Integration_Extraction(IntegrationTest):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()

        # Create test data
        account1 = cls.connection.Account.create({"Name": "Picon Fleet Headquarters"})
        cls.register_session_record("Account", account1["id"])
        account2 = cls.connection.Account.create({"Name": "Herakleides of Tauron"})
        cls.register_session_record("Account", account2["id"])
        account3 = cls.connection.Account.create({"Name": "Aerilon Agrinomics"})
        cls.register_session_record("Account", account3["id"])
        account4 = cls.connection.Account.create(
            {"Name": "Caprica Cosmetics", "ParentId": account3["id"]}
        )
        cls.register_session_record("Account", account4["id"])
        account5 = cls.connection.Account.create(
            {"Name": "Gemenon Gastronomy", "ParentId": account4["id"]}
        )
        cls.register_session_record("Account", account5["id"])
        contact = cls.connection.Contact.create(
            {"FirstName": "Elosha", "LastName": "Stark", "AccountId": account5["id"]}
        )
        cls.register_session_record("Contact", contact["id"])
        contact = cls.connection.Contact.create(
            {"FirstName": "Gaius", "LastName": "Baltar", "AccountId": account3["id"]}
        )
        cls.register_session_record("Contact", contact["id"])
        contact = cls.connection.Contact.create(
            {"FirstName": "Admiral", "LastName": "Nagata", "AccountId": account1["id"]}
        )
        cls.register_session_record("Contact", contact["id"])
        contact = cls.connection.Contact.create(
            {"FirstName": "Sam", "LastName": "Adama", "AccountId": account2["id"]}
        )
        cls.register_session_record("Contact", contact["id"])

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


class test_Integration_Load(IntegrationTest):
    def test_loads_single_object(self):
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
                "Name": "Caprica Corn",
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
            "SELECT Id, Name, IsActive, ProductCode FROM Product2"
        ).get("records")
        self.assertEqual(3, len(loaded_products))
        required_names = {x["Name"] for x in records}
        for r in loaded_products:
            self.register_case_record("Product2", r["Id"])
            self.assertIn(r["Name"], required_names)
            required_names.remove(r["Name"])

        self.assertEqual(0, len(required_names))

    def test_loads_complex_hierarchy(self):
        accounts = [
            {
                "Id": "001000000000001",
                "Name": "Tauron Tourist Commission",
                "ParentId": "",
            },
            {
                "Id": "001000000000002",
                "Name": "Emporion Enterprises",
                "ParentId": "001000000000001",
            },
            {
                "Id": "001000000000003AAA",
                "Name": "Caprica City Outreach",
                "ParentId": "001000000000001",
            },
        ]
        contacts = [
            {"Id": "003000000000000", "FirstName": "Joseph", "LastName": "Adama"},
            {"Id": "003000000000001", "FirstName": "Sam", "LastName": "Adama"},
        ]
        opportunities = [
            {
                "Id": "006000000000001",
                "AccountId": "001000000000001",
                "Name": "End-of-Year Promotion",
                "CloseDate": "2019-06-01",
                "StageName": "Closed Won",
            },
            {
                "Id": "006000000000002",
                "AccountId": "001000000000001",
                "Name": "New Initiative",
                "CloseDate": "2019-12-01",
                "StageName": "Closed Lost",
            },
        ]
        opportunity_contact_roles = [
            {
                "Id": "00K000000000001",
                "OpportunityId": "006000000000001",
                "ContactId": "003000000000000",
                "Role": "Decision Maker",
            },
            {
                "Id": "00K000000000001",
                "OpportunityId": "006000000000001",
                "ContactId": "003000000000000",
                "Role": "Influencer",
            },
            {
                "Id": "00K000000000002",
                "OpportunityId": "006000000000002",
                "ContactId": "003000000000001",
                "Role": "Decision Maker",
            },
        ]

        op = amaxa.LoadOperation(Connection(self.connection))
        op.file_store = MockFileStore()
        op.file_store.records["Account"] = accounts
        op.file_store.records["Contact"] = contacts
        op.file_store.records["Opportunity"] = opportunities
        op.file_store.records["OpportunityContactRole"] = opportunity_contact_roles

        op.add_step(amaxa.LoadStep("Account", set(["Name", "ParentId"])))
        op.add_step(
            amaxa.LoadStep("Contact", set(["AccountId", "FirstName", "LastName"]))
        )
        op.add_step(
            amaxa.LoadStep(
                "Opportunity", set(["AccountId", "Name", "CloseDate", "StageName"])
            )
        )
        op.add_step(
            amaxa.LoadStep(
                "OpportunityContactRole", set(["ContactId", "OpportunityId", "Role"])
            )
        )

        op.initialize()
        op.execute()

        loaded_accounts = self.connection.query_all(
            "SELECT Id, Name, (SELECT Name FROM ChildAccounts) FROM Account"
        ).get("records")
        self.assertEqual(len(accounts), len(loaded_accounts))
        required_names = [x["Name"] for x in accounts]
        for r in loaded_accounts:
            self.register_case_record("Account", r["Id"])
            self.assertIn(r["Name"], required_names)
            required_names.remove(r["Name"])
            if r["Name"] == "Tauron Tourist Commission":
                self.assertIsNotNone(r["ChildAccounts"])
                self.assertEqual(2, len(r["ChildAccounts"]["records"]))

        self.assertEqual(0, len(required_names))

        loaded_opportunities = self.connection.query_all(
            "SELECT Id, Name, (SELECT Id FROM OpportunityContactRoles) FROM Opportunity"
        ).get("records")
        self.assertEqual(len(opportunities), len(loaded_opportunities))
        required_names = [x["Name"] for x in opportunities]
        for r in loaded_opportunities:
            self.register_case_record("Opportunity", r["Id"])
            self.assertIn(r["Name"], required_names)
            required_names.remove(r["Name"])
            if r["Name"] == "End-of-Year Promotion":
                self.assertEqual(2, len(r["OpportunityContactRoles"]["records"]))
            else:
                self.assertEqual(1, len(r["OpportunityContactRoles"]["records"]))

        self.assertEqual(0, len(required_names))

        loaded_contacts = self.connection.query_all(
            "SELECT Id, FirstName, LastName, (SELECT Id FROM OpportunityContactRoles) FROM Contact"
        ).get("records")
        self.assertEqual(len(contacts), len(loaded_contacts))
        required_names = [x["LastName"] for x in contacts]
        for r in loaded_contacts:
            self.register_case_record("Contact", r["Id"])
            self.assertIn(r["LastName"], required_names)
            required_names.remove(r["LastName"])
            if r["FirstName"] == "Sam":
                self.assertEqual(1, len(r["OpportunityContactRoles"]["records"]))
            else:
                self.assertEqual(2, len(r["OpportunityContactRoles"]["records"]))

        self.assertEqual(0, len(required_names))
