import unittest
import simple_salesforce
import io
from unittest.mock import Mock
from .MockSimpleSalesforce import MockSimpleSalesforce
from .. import amaxa, loader, constants


class test_LoadOperationLoader(unittest.TestCase):
    def _mock_execute(self, context, input_data):
        if input_data is not None:

            def open_func(key, status):
                return io.StringIO(input_data[key])

            open_mock = Mock(side_effect=open_func)
            with unittest.mock.patch("builtins.open", open_mock):
                context.load()
        else:
            context._open_files = Mock()
            context._validate_input_file_columns = Mock()
            context.load()

    def _run_error_validating_test(self, ex, error_list, sf_mock=None, input_data=None):
        context = loader.LoadOperationLoader(ex, sf_mock or MockSimpleSalesforce())

        self._mock_execute(context, input_data)

        self.assertEqual(error_list, context.errors)
        self.assertIsNone(context.result)

    def _run_success_test(self, ex, sf_mock=None, input_data=None):
        context = loader.LoadOperationLoader(ex, sf_mock or MockSimpleSalesforce())

        self._mock_execute(context, input_data)

        self.assertEqual([], context.errors)
        self.assertIsInstance(context.result, amaxa.LoadOperation)

        return context.result

    @unittest.mock.patch("simple_salesforce.Salesforce")
    def test_load_traps_login_exceptions(self, sf_mock):
        return_exception = simple_salesforce.SalesforceAuthenticationFailed(
            500, "Internal Server Error"
        )
        sf_mock.describe = Mock(side_effect=return_exception)

        ex = {
            "version": 1,
            "operation": [
                {
                    "sobject": "Account",
                    "field-group": "writeable",
                    "extract": {"all": True},
                }
            ],
        }
        self._run_error_validating_test(
            ex,
            ["Unable to authenticate to Salesforce: {}".format(return_exception)],
            sf_mock,
        )

    def test_LoadOperationLoader_finds_writeable_field_group(self):
        ex = {
            "version": 1,
            "operation": [
                {
                    "sobject": "Account",
                    "field-group": "writeable",
                    "extract": {"all": True},
                    "input-validation": "none",
                }
            ],
        }

        result = self._run_success_test(ex)

        self.assertEqual(1, len(result.steps))
        self.assertEqual(
            set(result.get_filtered_field_map("Account", lambda x: x["createable"])),
            result.steps[0].field_scope,
        )

    def test_LoadOperationLoader_field_groups_omit_unsupported_types(self):
        ex = {
            "version": 1,
            "operation": [
                {
                    "sobject": "Account",
                    "field-group": "writeable",
                    "extract": {"all": True},
                    "input-validation": "none",
                }
            ],
        }

        result = self._run_success_test(ex)

        self.assertEqual(1, len(result.steps))
        self.assertNotIn("ShippingAddress", result.steps[0].field_scope)

    def test_LoadOperationLoader_generates_field_list(self):
        ex = {
            "version": 1,
            "operation": [
                {
                    "sobject": "Account",
                    "fields": ["Name", "Industry"],
                    "extract": {"all": True},
                    "input-validation": "none",
                }
            ],
        }

        result = self._run_success_test(ex)
        self.assertEqual({"Name", "Industry"}, result.steps[0].field_scope)

    def test_LoadOperationLoader_respects_none_validation_option(self):
        ex = {
            "version": 1,
            "operation": [
                {
                    "sobject": "Account",
                    "fields": ["Name", "Industry"],
                    "extract": {"all": True},
                    "input-validation": "none",
                }
            ],
        }

        result = self._run_success_test(
            ex,
            input_data={
                "Account.csv": "Id,Name,Industry,Description",
                "Account-results.csv": "",
            },
        )
        self.assertEqual({"Name", "Industry"}, result.steps[0].field_scope)

    def test_LoadOperationLoader_validates_file_against_field_scope_excess_fields(self):
        ex = {
            "version": 1,
            "operation": [
                {
                    "sobject": "Account",
                    "fields": ["Name", "Industry"],
                    "extract": {"all": True},
                }
            ],
        }

        fieldnames = ["Id", "Name", "Industry", "Description"]
        self._run_error_validating_test(
            ex,
            [
                "Input file for sObject {} does not match specified field scope.\nScope: {}\nFile Columns: {}\n".format(
                    "Account",
                    ", ".join(sorted(["Name", "Industry"])),
                    ", ".join(sorted(set(fieldnames) - set(["Id"]))),
                )
            ],
            input_data={"Account.csv": ",".join(fieldnames), "Account-results.csv": ""},
        )

    def test_LoadOperationLoader_validates_file_against_field_scope_missing_fields(
        self
    ):
        ex = {
            "version": 1,
            "operation": [
                {
                    "sobject": "Account",
                    "fields": ["Name", "Industry"],
                    "extract": {"all": True},
                }
            ],
        }

        fieldnames = ["Id", "Name"]
        self._run_error_validating_test(
            ex,
            [
                "Input file for sObject {} does not match specified field scope.\nScope: {}\nFile Columns: {}\n".format(
                    "Account",
                    ", ".join(sorted(["Name", "Industry"])),
                    ", ".join(sorted(set(fieldnames) - set(["Id"]))),
                )
            ],
            input_data={"Account.csv": ",".join(fieldnames), "Account-results.csv": ""},
        )

    def test_LoadOperationLoader_validates_file_against_field_group(self):
        ex = {
            "version": 1,
            "operation": [
                {
                    "sobject": "Account",
                    "field-group": "writeable",
                    "extract": {"all": True},
                }
            ],
        }

        fieldnames = ["Id", "Name", "Industry", "Test__c"]
        self._run_error_validating_test(
            ex,
            [
                "Input file for sObject {} contains excess columns over field group '{}': {}".format(
                    "Account", "writeable", "Test__c"
                )
            ],
            input_data={"Account.csv": ",".join(fieldnames), "Account-results.csv": ""},
        )

    def test_LoadOperationLoader_validates_file_against_field_group_with_strict(self):
        ex = {
            "version": 1,
            "operation": [
                {
                    "sobject": "Account",
                    "field-group": "writeable",
                    "extract": {"all": True},
                    "input-validation": "strict",
                }
            ],
        }

        fieldnames = ["Id", "Name"]
        context_fieldnames = (
            amaxa.LoadOperation(MockSimpleSalesforce())
            .get_filtered_field_map(
                "Account",
                lambda f: f["createable"]
                and f["type"] not in ["location", "address", "base64"],
            )
            .keys()
        )
        self._run_error_validating_test(
            ex,
            [
                "Input file for sObject {} does not match specified field scope.\nScope: {}\nFile Columns: {}\n".format(
                    "Account",
                    ", ".join(sorted(context_fieldnames)),
                    ", ".join(sorted(set(fieldnames) - set(["Id"]))),
                )
            ],
            input_data={
                "Account.csv": ",".join(fieldnames) + "\n",
                "Account-results.csv": "",
            },
        )

    def test_LoadOperationLoader_returns_error_unsupported_fields(self):
        ex = {
            "version": 1,
            "operation": [
                {
                    "sobject": "Attachment",
                    "fields": ["Name", "Body"],
                    "extract": {"all": True},
                }
            ],
        }

        self._run_error_validating_test(
            ex,
            [
                "Field {}.{} is of an unsupported type (base64)".format(
                    "Attachment", "Body"
                )
            ],
        )

    def test_LoadOperationLoader_populates_lookup_behaviors(self):
        ex = {
            "version": 1,
            "operation": [
                {
                    "sobject": "Account",
                    "fields": [
                        "Name",
                        {"field": "ParentId", "self-lookup-behavior": "trace-none"},
                    ],
                    "extract": {"all": True},
                    "input-validation": "none",
                },
                {
                    "sobject": "Task",
                    "fields": [
                        {"field": "WhoId", "outside-lookup-behavior": "drop-field"}
                    ],
                    "extract": {"descendents": True},
                    "input-validation": "none",
                },
            ],
        }

        result = self._run_success_test(ex)

        self.assertEqual(
            amaxa.SelfLookupBehavior.TRACE_NONE,
            result.steps[0].get_lookup_behavior_for_field("ParentId"),
        )
        self.assertEqual(
            amaxa.OutsideLookupBehavior.DROP_FIELD,
            result.steps[1].get_lookup_behavior_for_field("WhoId"),
        )

    def test_LoadOperationLoader_validates_lookup_behaviors_for_self_lookups(self):
        ex = {
            "version": 1,
            "operation": [
                {
                    "sobject": "Account",
                    "fields": [
                        "Name",
                        {"field": "ParentId", "outside-lookup-behavior": "include"},
                    ],
                    "extract": {"all": True},
                }
            ],
        }

        self._run_error_validating_test(
            ex,
            [
                "Lookup behavior '{}' specified for field {}.{} is not valid for this lookup type.".format(
                    "include", "Account", "ParentId"
                )
            ],
        )

    def test_LoadOperationLoader_validates_lookup_behaviors_for_dependent_lookups(self):
        ex = {
            "version": 1,
            "operation": [
                {
                    "sobject": "Task",
                    "fields": [
                        {"field": "WhatId", "self-lookup-behavior": "trace-all"}
                    ],
                    "extract": {"all": True},
                    "input-validation": "none",
                },
                {
                    "sobject": "Account",
                    "fields": ["Name"],
                    "extract": {"all": True},
                    "input-validation": "none",
                },
            ],
        }

        self._run_error_validating_test(
            ex,
            [
                "Lookup behavior '{}' specified for field {}.{} is not valid for this lookup type.".format(
                    "trace-all", "Task", "WhatId"
                )
            ],
        )

    @unittest.mock.patch("logging.getLogger")
    def test_LoadOperationLoader_warns_lookups_other_objects(self, logger):
        amaxa_logger = Mock()
        logger.return_value = amaxa_logger

        ex = {
            "version": 1,
            "operation": [
                {
                    "sobject": "Account",
                    "fields": ["OwnerId"],
                    "extract": {"all": True},
                    "input-validation": "none",
                }
            ],
        }

        self._run_success_test(ex)

        amaxa_logger.warning.assert_called_once_with(
            "Field %s.%s is a reference none of whose targets (%s) are included in the operation. Reference handlers will be inactive for references to non-included sObjects.",
            "Account",
            "OwnerId",
            "User",
        )

    def test_validate_load_flags_missing_fields(self):
        ex = {
            "version": 1,
            "operation": [
                {
                    "sobject": "Account",
                    "fields": ["Name", "Test__c"],
                    "extract": {"all": True},
                }
            ],
        }

        self._run_error_validating_test(
            ex,
            [
                "Field Account.Test__c does not exist or does not have the correct CRUD permission (createable)."
            ],
        )

    def test_validate_load_flags_non_updateable_dependent_fields(self):
        mock_sf = MockSimpleSalesforce()
        for field in mock_sf.get_describe("Account")["fields"]:
            if field["name"] == "ParentId":
                field["updateable"] = False

        ex = {
            "version": 1,
            "operation": [
                {
                    "sobject": "Account",
                    "fields": ["Name", "ParentId"],
                    "extract": {"all": True},
                }
            ],
        }

        self._run_error_validating_test(
            ex,
            [
                "Field {}.{} is a dependent lookup, but is not updateable.".format(
                    "Account", "ParentId"
                )
            ],
            sf_mock=mock_sf,
        )

    def test_LoadOperationLoader_creates_valid_steps_with_files(self):
        context = amaxa.LoadOperation(MockSimpleSalesforce())
        context.add_dependency = Mock()

        ex = {
            "version": 1,
            "operation": [
                {
                    "sobject": "Account",
                    "fields": ["Name"],
                    "extract": {"all": True},
                    "input-validation": "none",
                },
                {
                    "sobject": "Contact",
                    "fields": ["LastName"],
                    "extract": {"all": True},
                    "input-validation": "none",
                },
                {
                    "sobject": "Opportunity",
                    "fields": ["StageName"],
                    "extract": {"all": True},
                    "input-validation": "none",
                },
                {
                    "sobject": "Task",
                    "fields": ["Subject"],
                    "extract": {"all": True},
                    "input-validation": "none",
                },
            ],
        }

        context = loader.LoadOperationLoader(ex, MockSimpleSalesforce())
        m = unittest.mock.mock_open()
        with unittest.mock.patch("builtins.open", m):
            context.load()

        self.assertEqual([], context.errors)
        self.assertIsInstance(context.result, amaxa.LoadOperation)

        m.assert_has_calls(
            [
                unittest.mock.call("Account.csv", "r"),
                unittest.mock.call("Contact.csv", "r"),
                unittest.mock.call("Opportunity.csv", "r"),
                unittest.mock.call("Task.csv", "r"),
                unittest.mock.call("Account-results.csv", "w"),
                unittest.mock.call("Contact-results.csv", "w"),
                unittest.mock.call("Opportunity-results.csv", "w"),
                unittest.mock.call("Task-results.csv", "w"),
            ],
            any_order=True,
        )

        self.assertEqual(4, len(context.result.steps))
        self.assertEqual("Account", context.result.steps[0].sobjectname)
        self.assertEqual("Contact", context.result.steps[1].sobjectname)
        self.assertEqual("Opportunity", context.result.steps[2].sobjectname)
        self.assertEqual("Task", context.result.steps[3].sobjectname)

    @unittest.mock.patch("csv.DictWriter.writeheader")
    def test_LoadOperationLoader_writes_csv_headers(self, dict_writer):
        ex = {
            "version": 1,
            "operation": [
                {
                    "sobject": "Account",
                    "fields": ["Name"],
                    "extract": {"all": True},
                    "input-validation": "none",
                }
            ],
        }

        context = loader.LoadOperationLoader(ex, MockSimpleSalesforce())
        m = unittest.mock.mock_open()
        with unittest.mock.patch("builtins.open", m):
            context.load()

        self.assertEqual([], context.errors)
        self.assertIsInstance(context.result, amaxa.LoadOperation)

        m.assert_has_calls(
            [
                unittest.mock.call("Account.csv", "r"),
                unittest.mock.call("Account-results.csv", "w"),
            ],
            any_order=True,
        )
        dict_writer.assert_called_once_with()

    @unittest.mock.patch("csv.DictWriter.writeheader")
    def test_LoadOperationLoader_does_not_truncate_or_write_headers_on_resume(
        self, dict_writer
    ):
        ex = {
            "version": 1,
            "operation": [
                {
                    "sobject": "Account",
                    "fields": ["Name"],
                    "extract": {"all": True},
                    "input-validation": "none",
                }
            ],
        }

        context = loader.LoadOperationLoader(ex, MockSimpleSalesforce(), True)
        m = unittest.mock.mock_open()
        with unittest.mock.patch("builtins.open", m):
            context.load()

        self.assertEqual([], context.errors)
        self.assertIsInstance(context.result, amaxa.LoadOperation)

        m.assert_has_calls(
            [
                unittest.mock.call("Account.csv", "r"),
                unittest.mock.call("Account-results.csv", "a"),
            ],
            any_order=True,
        )
        dict_writer.assert_not_called()

    def test_LoadOperationLoader_populates_options(self):
        result = self._run_success_test(
            {
                "version": 2,
                "options": {"bulk-api-batch-size": 9000},
                "operation": [
                    {
                        "sobject": "Account",
                        "fields": ["Name"],
                        "extract": {"all": True},
                    },
                    {
                        "sobject": "Task",
                        "options": {"bulk-api-batch-size": 10000},
                        "fields": [{"field": "Subject"}],
                        "extract": {"all": True},
                    },
                ],
            }
        )

        self.assertEqual(9000, result.steps[0].get_option("bulk-api-batch-size"))
        self.assertEqual(10000, result.steps[1].get_option("bulk-api-batch-size"))

    def test_LoadOperationLoader_populates_default_options(self):
        result = self._run_success_test(
            {
                "version": 2,
                "operation": [
                    {"sobject": "Account", "fields": ["Name"], "extract": {"all": True}}
                ],
            }
        )

        self.assertEqual(
            constants.OPTION_DEFAULTS["bulk-api-batch-size"],
            result.steps[0].get_option("bulk-api-batch-size"),
        )
