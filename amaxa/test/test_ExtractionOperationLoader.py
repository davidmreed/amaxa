import io
import unittest
import simple_salesforce
from unittest.mock import Mock
from .MockSimpleSalesforce import MockSimpleSalesforce
from .. import amaxa, loader, constants


class test_ExtractionOperationLoader(unittest.TestCase):
    def _mock_execute(self, context, file_data):
        if file_data is not None:

            def open_func(key, status):
                return io.StringIO(file_data[key])

            open_mock = Mock(side_effect=open_func)
            with unittest.mock.patch("builtins.open", open_mock):
                context.load()
        else:
            context._open_files = Mock()
            context.load()

    def _run_error_validating_test(self, ex, error_list, sf_mock=None, file_data=None):
        context = loader.ExtractionOperationLoader(
            ex, sf_mock or MockSimpleSalesforce()
        )

        self._mock_execute(context, file_data)

        self.assertEqual(error_list, context.errors)
        self.assertIsNone(context.result)

    def _run_success_test(self, ex, sf_mock=None, file_data=None):
        context = loader.ExtractionOperationLoader(
            ex, sf_mock or MockSimpleSalesforce()
        )

        self._mock_execute(context, file_data)

        self.assertEqual([], context.errors)
        self.assertIsInstance(context.result, amaxa.ExtractOperation)

        return context.result

    @unittest.mock.patch("simple_salesforce.Salesforce")
    def test_load_extraction_operation_traps_login_exceptions(self, sf_mock):
        return_exception = simple_salesforce.SalesforceAuthenticationFailed(
            500, "Internal Server Error"
        )
        sf_mock.describe = Mock(side_effect=return_exception)

        self._run_error_validating_test(
            {
                "version": 1,
                "operation": [
                    {
                        "sobject": "Account",
                        "field-group": "readable",
                        "extract": {"all": True},
                    }
                ],
            },
            ["Unable to authenticate to Salesforce: {}".format(return_exception)],
            sf_mock,
        )

    def test_ExtractionOperationLoader_returns_error_on_bad_ids(self):
        self._run_error_validating_test(
            {
                "version": 1,
                "operation": [
                    {
                        "sobject": "Account",
                        "fields": ["Name"],
                        "extract": {"ids": ["001XXXXXXXXXXXXXXXXX", ""]},
                    }
                ],
            },
            ["One or more invalid Id values provided for sObject Account"],
        )

    def test_load_extraction_operation_flags_missing_sobjects(self):
        self._run_error_validating_test(
            {
                "version": 1,
                "operation": [
                    {
                        "sobject": "Account",
                        "fields": ["Name"],
                        "extract": {"all": True},
                    },
                    {
                        "sobject": "Test__c",
                        "fields": ["Name"],
                        "extract": {"all": True},
                    },
                ],
            },
            [
                "sObject Test__c does not exist or does not have the correct permission (queryable)"
            ],
        )

    def test_load_extraction_operation_flags_missing_fields(self):
        self._run_error_validating_test(
            {
                "version": 1,
                "operation": [
                    {
                        "sobject": "Account",
                        "fields": ["Name", "Test__c"],
                        "extract": {"all": True},
                    }
                ],
            },
            [
                "Field Account.Test__c does not exist or does not have the correct CRUD permission."
            ],
        )

    def test_load_extraction_operation_finds_readable_field_group(self):
        result = self._run_success_test(
            {
                "version": 1,
                "operation": [
                    {
                        "sobject": "Account",
                        "field-group": "readable",
                        "extract": {"all": True},
                    }
                ],
            }
        )

        self.assertEqual(1, len(result.steps))
        self.assertEqual(
            set(
                result.get_filtered_field_map(
                    "Account", lambda x: x["type"] != "address"
                )
            ),
            result.steps[0].field_scope,
        )

    def test_load_extraction_operation_finds_writeable_field_group(self):
        result = self._run_success_test(
            {
                "version": 1,
                "operation": [
                    {
                        "sobject": "Account",
                        "field-group": "writeable",
                        "extract": {"all": True},
                    }
                ],
            }
        )

        self.assertEqual(1, len(result.steps))
        self.assertEqual(
            set(result.get_filtered_field_map("Account", lambda x: x["createable"]))
            | set(["Id"]),
            result.steps[0].field_scope,
        )

    def test_load_extraction_operation_readable_field_group_omits_unsupported_types(
        self
    ):
        result = self._run_success_test(
            {
                "version": 1,
                "operation": [
                    {
                        "sobject": "Account",
                        "field-group": "readable",
                        "extract": {"all": True},
                    }
                ],
            }
        )

        self.assertEqual(1, len(result.steps))
        self.assertNotIn("BillingAddress", result.steps[0].field_scope)

    def test_load_extraction_operation_writeable_field_group_omits_unsupported_types(
        self
    ):
        result = self._run_success_test(
            {
                "version": 1,
                "operation": [
                    {
                        "sobject": "Attachment",
                        "field-group": "writeable",
                        "extract": {"all": True},
                    }
                ],
            }
        )

        self.assertEqual(1, len(result.steps))
        self.assertNotIn("Body", result.steps[0].field_scope)

    def test_load_extraction_operation_generates_field_list(self):
        result = self._run_success_test(
            {
                "version": 1,
                "operation": [
                    {
                        "sobject": "Account",
                        "fields": ["Name", "Industry"],
                        "extract": {"all": True},
                    }
                ],
            }
        )

        self.assertEqual({"Name", "Industry", "Id"}, result.steps[0].field_scope)

    def test_load_extraction_operation_creates_export_mapper(self):
        result = self._run_success_test(
            {
                "version": 1,
                "operation": [
                    {
                        "sobject": "Account",
                        "fields": [
                            {
                                "field": "Name",
                                "column": "Account Name",
                                "transforms": ["strip", "lowercase"],
                            },
                            "Industry",
                        ],
                        "extract": {"all": True},
                    }
                ],
            }
        )

        self.assertEqual({"Name", "Industry", "Id"}, result.steps[0].field_scope)
        self.assertIn("Account", result.mappers)

        mapper = result.mappers["Account"]
        self.assertEqual(
            {"Account Name": "university of caprica", "Industry": "Education"},
            mapper.transform_record(
                {"Name": "UNIversity of caprica  ", "Industry": "Education"}
            ),
        )

    def test_load_extraction_operation_returns_error_base64_fields(self):
        self._run_error_validating_test(
            {
                "version": 1,
                "operation": [
                    {
                        "sobject": "Attachment",
                        "fields": ["Name", "Body"],
                        "extract": {"all": True},
                    }
                ],
            },
            [
                "Field {}.{} is of an unsupported type (base64)".format(
                    "Attachment", "Body"
                )
            ],
        )

    @unittest.mock.patch("logging.getLogger")
    def test_load_extraction_operation_warns_lookups_other_objects(self, logger):
        amaxa_logger = Mock()
        logger.return_value = amaxa_logger
        self._run_success_test(
            {
                "version": 1,
                "operation": [
                    {
                        "sobject": "Contact",
                        "fields": ["AccountId"],
                        "extract": {"all": True},
                    }
                ],
            }
        )

        amaxa_logger.warning.assert_called_once_with(
            "Field %s.%s is a reference none of whose targets (%s) are included in the operation. Reference handlers will be inactive for references to non-included sObjects.",
            "Contact",
            "AccountId",
            ", ".join(["Account"]),
        )

    def test_load_extraction_operation_populates_lookup_behaviors(self):
        result = self._run_success_test(
            {
                "version": 1,
                "operation": [
                    {
                        "sobject": "Account",
                        "fields": [
                            "Name",
                            {"field": "ParentId", "self-lookup-behavior": "trace-none"},
                        ],
                        "extract": {"all": True},
                    },
                    {
                        "sobject": "Task",
                        "fields": [
                            {"field": "WhatId", "outside-lookup-behavior": "drop-field"}
                        ],
                        "extract": {"all": True},
                    },
                ],
            }
        )

        self.assertEqual(
            amaxa.SelfLookupBehavior.TRACE_NONE,
            result.steps[0].get_self_lookup_behavior_for_field("ParentId"),
        )
        self.assertEqual(
            amaxa.OutsideLookupBehavior.DROP_FIELD,
            result.steps[1].get_outside_lookup_behavior_for_field("WhatId"),
        )

    def test_load_extraction_operation_validates_lookup_behaviors_for_self_lookups(
        self
    ):
        self._run_error_validating_test(
            {
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
            },
            [
                "Lookup behavior '{}' specified for field {}.{} is not valid for this lookup type.".format(
                    "include", "Account", "ParentId"
                )
            ],
        )

    def test_load_extraction_operation_validates_lookup_behaviors_for_dependent_lookups(
        self
    ):
        self._run_error_validating_test(
            {
                "version": 1,
                "operation": [
                    {
                        "sobject": "Contact",
                        "fields": [
                            {"field": "AccountId", "self-lookup-behavior": "trace-all"}
                        ],
                        "extract": {"all": True},
                    },
                    {
                        "sobject": "Account",
                        "fields": ["Name"],
                        "extract": {"all": True},
                    },
                ],
            },
            [
                "Lookup behavior '{}' specified for field {}.{} is not valid for this lookup type.".format(
                    "trace-all", "Contact", "AccountId"
                )
            ],
        )

    def test_load_extraction_operation_creates_valid_steps_with_files(self):
        ex = {
            "version": 1,
            "operation": [
                {"sobject": "Account", "fields": ["Name"], "extract": {"all": True}},
                {
                    "sobject": "Contact",
                    "fields": ["Name"],
                    "extract": {"ids": ["003000000000000", "003000000000001"]},
                },
                {
                    "sobject": "Opportunity",
                    "fields": ["Name"],
                    "extract": {"descendents": True},
                },
                {
                    "sobject": "Task",
                    "fields": ["Id"],
                    "extract": {"query": "AccountId != null"},
                },
            ],
        }
        operation_loader = loader.ExtractionOperationLoader(ex, MockSimpleSalesforce())

        m = unittest.mock.mock_open()
        with unittest.mock.patch("builtins.open", m):
            operation_loader.load()

        result = operation_loader.result

        self.assertEqual([], operation_loader.errors)
        self.assertIsInstance(result, amaxa.ExtractOperation)

        m.assert_has_calls(
            [
                unittest.mock.call("Account.csv", "w"),
                unittest.mock.call("Contact.csv", "w"),
                unittest.mock.call("Opportunity.csv", "w"),
                unittest.mock.call("Task.csv", "w"),
            ],
            any_order=True,
        )

        # FIXME: unit test this separately.
        # context.add_dependency.assert_has_calls(
        #    [
        #        unittest.mock.call('Contact', amaxa.SalesforceId('003000000000000')),
        #        unittest.mock.call('Contact', amaxa.SalesforceId('003000000000001'))
        #    ]
        # )

        self.assertEqual(4, len(result.steps))
        self.assertEqual("Account", result.steps[0].sobjectname)
        self.assertEqual(amaxa.ExtractionScope.ALL_RECORDS, result.steps[0].scope)
        self.assertEqual("Contact", result.steps[1].sobjectname)
        self.assertEqual(amaxa.ExtractionScope.SELECTED_RECORDS, result.steps[1].scope)
        self.assertEqual("Opportunity", result.steps[2].sobjectname)
        self.assertEqual(amaxa.ExtractionScope.DESCENDENTS, result.steps[2].scope)
        self.assertEqual("Task", result.steps[3].sobjectname)
        self.assertEqual(amaxa.ExtractionScope.QUERY, result.steps[3].scope)

    @unittest.mock.patch("csv.DictWriter.writeheader")
    def test_load_extraction_operation_writes_correct_headers(self, dict_writer):
        context = self._run_success_test(
            {
                "version": 1,
                "operation": [
                    {
                        "sobject": "Account",
                        "fields": ["Name", "ParentId", "Id"],
                        "extract": {"all": True},
                    }
                ],
            },
            file_data={"Account.csv": ""},
        )

        # m.assert_called_once_with('Account.csv', 'w') #FIXME
        csv_file = context.file_store.get_csv("Account", amaxa.FileType.OUTPUT)
        self.assertIsNotNone(csv_file)

        dict_writer.assert_called_once_with()
        self.assertEqual(["Id", "Name", "ParentId"], csv_file.fieldnames)

    def test_load_extraction_populates_options(self):
        result = self._run_success_test(
            {
                "version": 2,
                "options": {
                    "bulk-api-batch-size": 9000,
                },
                "operation": [
                    {
                        "sobject": "Account",
                        "fields": [
                            "Name",
                        ],
                        "extract": {"all": True},
                    },
                    {
                        "sobject": "Task",
                        "options": {
                            "bulk-api-batch-size": 10000,
                        },
                        "fields": [
                            {"field": "Subject"}
                        ],
                        "extract": {"all": True},
                    },
                ],
            }
        )

        self.assertEqual(9000, result.steps[0].get_option("bulk-api-batch-size"))
        self.assertEqual(10000, result.steps[1].get_option("bulk-api-batch-size"))

    def test_load_extraction_populates_default_options(self):
        result = self._run_success_test(
            {
                "version": 2,
                "operation": [
                    {
                        "sobject": "Account",
                        "fields": [
                            "Name"
                        ],
                        "extract": {"all": True},
                    },
                ],
            }
        )

        self.assertEqual(constants.OPTION_DEFAULTS["bulk-api-batch-size"], result.steps[0].get_option("bulk-api-batch-size"))
