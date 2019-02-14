import unittest
import simple_salesforce
import io
from unittest.mock import Mock
from .MockSimpleSalesforce import MockSimpleSalesforce
from .. import amaxa, loader


class test_load_load_operation(unittest.TestCase):
    def test_load_load_operation_returns_validation_errors(self):
        context = Mock()
        (result, errors) = loader.load_load_operation(
            {
                'operation': [
                    { 
                        'sobject': 'Account',
                        'fields': [ 'Name', 'ParentId' ],
                        'extract': { 'all': True }
                    }
                ]
            },
            context
        )

        self.assertIsNone(result)
        self.assertEqual(['version: [\'required field\']'], errors)
        context.assert_not_called()

    @unittest.mock.patch('simple_salesforce.Salesforce')
    def test_load_load_operation_traps_login_exceptions(self, sf_mock):
        return_exception = simple_salesforce.SalesforceAuthenticationFailed(500, 'Internal Server Error')
        sf_mock.describe = Mock(side_effect=return_exception)
        context = amaxa.LoadOperation(sf_mock)
        ex = {
            'version': 1,
            'operation': [
                { 
                    'sobject': 'Account',
                    'field-group': 'writeable',
                    'extract': { 'all': True }
                }
            ]
        }

        (result, errors) = loader.load_load_operation(ex, context)

        self.assertIsNone(result)
        self.assertEqual(['Unable to authenticate to Salesforce: {}'.format(return_exception)], errors)

    def test_load_load_operation_flags_missing_sobjects(self):
        context = Mock()
        context.steps = []
        context.connection = MockSimpleSalesforce()

        ex = { 
            'version': 1, 
            'operation': [
                { 
                    'sobject': 'Object__c',
                    'fields': [ 'Name' ],
                    'extract': { 'all': True }
                }
            ]
        }
        m = unittest.mock.mock_open()
        with unittest.mock.patch('builtins.open', m):
            (result, errors) = loader.load_load_operation(ex, context)
        
        m.assert_not_called()

        self.assertIsNone(result)
        self.assertEqual(
            [
                'sObject Object__c does not exist, is not visible, or is not createable.'
            ],
            errors
        )
    def test_validate_load_flags_missing_fields(self):
        context = amaxa.LoadOperation(MockSimpleSalesforce())

        ex = { 
            'version': 1, 
            'operation': [
                { 
                    'sobject': 'Account',
                    'fields': [ 'Name', 'Test__c' ],
                    'extract': { 'all': True }
                }
            ]
        }

        m = unittest.mock.mock_open()
        with unittest.mock.patch('builtins.open', m):
            (result, errors) = loader.load_load_operation(ex, context)

        m.assert_not_called()

        self.assertIsNone(result)
        self.assertEqual(
            [
                'Field Account.Test__c does not exist, is not writeable, or is not visible.'
            ],
            errors
        )

    def test_validate_load_flags_non_writeable_fields(self):
        context = amaxa.LoadOperation(MockSimpleSalesforce())

        ex = { 
            'version': 1, 
            'operation': [
                { 
                    'sobject': 'Account',
                    'fields': [ 'Name', 'IsDeleted' ],
                    'extract': { 'all': True }
                }
            ]
        }

        m = unittest.mock.mock_open()
        with unittest.mock.patch('builtins.open', m):
            (result, errors) = loader.load_load_operation(ex, context)

        m.assert_not_called()

        self.assertIsNone(result)
        self.assertEqual(
            [
                'Field Account.IsDeleted does not exist, is not writeable, or is not visible.'
            ],
            errors
        )

    def test_validate_load_flags_non_updateable_dependent_fields(self):
        context = amaxa.LoadOperation(MockSimpleSalesforce())
        context.get_field_map('Account')['ParentId']['updateable'] = False

        ex = { 
            'version': 1, 
            'operation': [
                { 
                    'sobject': 'Account',
                    'fields': [ 'Name', 'ParentId' ],
                    'extract': { 'all': True }
                }
            ]
        }

        m = unittest.mock.mock_open()
        with unittest.mock.patch('builtins.open', m):
            (result, errors) = loader.load_load_operation(ex, context)

        m.assert_not_called()

        self.assertEqual(
            [
                'Field {}.{} is a dependent lookup, but is not updateable.'.format('Account', 'ParentId')
            ],
            errors
        )
        self.assertIsNone(result)

    def test_load_load_operation_creates_valid_steps_with_files(self):
        context = amaxa.LoadOperation(MockSimpleSalesforce())
        context.add_dependency = Mock()

        ex = {
            'version': 1,
            'operation': [
                { 
                    'sobject': 'Account',
                    'fields': [ 'Name' ],
                    'extract': { 'all': True },
                    'input-validation': 'none'
                },
                { 
                    'sobject': 'Contact',
                    'fields': [ 'LastName' ],
                    'extract': { 'all': True },
                    'input-validation': 'none'
                },
                {
                    'sobject': 'Opportunity',
                    'fields': [ 'StageName' ],
                    'extract': { 'all': True },
                    'input-validation': 'none'
                },
                {
                    'sobject': 'Task',
                    'fields': [ 'Subject' ],
                    'extract': { 'all': True },
                    'input-validation': 'none'
                }

            ]
        }

        m = unittest.mock.mock_open()
        with unittest.mock.patch('builtins.open', m):
            (result, errors) = loader.load_load_operation(ex, context)

        self.assertEqual([], errors)
        self.assertIsInstance(result, amaxa.LoadOperation)

        m.assert_has_calls(
            [
                unittest.mock.call('Account.csv', 'r'),
                unittest.mock.call('Contact.csv', 'r'),
                unittest.mock.call('Opportunity.csv', 'r'),
                unittest.mock.call('Task.csv', 'r'),
                unittest.mock.call('Account-results.csv', 'w'),
                unittest.mock.call('Contact-results.csv', 'w'),
                unittest.mock.call('Opportunity-results.csv', 'w'),
                unittest.mock.call('Task-results.csv', 'w')
            ],
            any_order=True
        )

        self.assertEqual(4, len(result.steps))
        self.assertEqual('Account', result.steps[0].sobjectname)
        self.assertEqual('Contact', result.steps[1].sobjectname)
        self.assertEqual('Opportunity', result.steps[2].sobjectname)
        self.assertEqual('Task', result.steps[3].sobjectname)

    @unittest.mock.patch('csv.DictWriter.writeheader')
    def test_load_load_operation_writes_csv_headers(self, dict_writer):
        context = amaxa.LoadOperation(MockSimpleSalesforce())
        context.add_dependency = Mock()

        ex = {
            'version': 1,
            'operation': [
                { 
                    'sobject': 'Account',
                    'fields': [ 'Name' ],
                    'extract': { 'all': True },
                    'input-validation': 'none'
                }
            ]
        }

        m = unittest.mock.mock_open()
        with unittest.mock.patch('builtins.open', m):
            (result, errors) = loader.load_load_operation(ex, context)

        self.assertEqual([], errors)
        self.assertIsInstance(result, amaxa.LoadOperation)

        m.assert_has_calls(
            [
                unittest.mock.call('Account.csv', 'r'),
                unittest.mock.call('Account-results.csv', 'w'),
            ],
            any_order=True
        )
        dict_writer.assert_called_once_with()

    @unittest.mock.patch('csv.DictWriter.writeheader')
    def test_load_load_operation_does_not_truncate_or_write_headers_on_resume(self, dict_writer):
        context = amaxa.LoadOperation(MockSimpleSalesforce())
        context.add_dependency = Mock()

        ex = {
            'version': 1,
            'operation': [
                { 
                    'sobject': 'Account',
                    'fields': [ 'Name' ],
                    'extract': { 'all': True },
                    'input-validation': 'none'
                }
            ]
        }

        m = unittest.mock.mock_open()
        with unittest.mock.patch('builtins.open', m):
            (result, errors) = loader.load_load_operation(ex, context, True)

        self.assertEqual([], errors)
        self.assertIsInstance(result, amaxa.LoadOperation)

        m.assert_has_calls(
            [
                unittest.mock.call('Account.csv', 'r'),
                unittest.mock.call('Account-results.csv', 'a'),
            ],
            any_order=True
        )
        dict_writer.assert_not_called()

    def test_load_load_operation_finds_writeable_field_group(self):
        context = amaxa.LoadOperation(MockSimpleSalesforce())

        ex = {
            'version': 1,
            'operation': [
                { 
                    'sobject': 'Account',
                    'field-group': 'writeable',
                    'extract': { 'all': True },
                    'input-validation': 'none'
                }
            ]
        }

        m = unittest.mock.mock_open()
        with unittest.mock.patch('builtins.open', m):
            (result, errors) = loader.load_load_operation(ex, context)

        self.assertEqual([], errors)
        self.assertIsInstance(result, amaxa.LoadOperation)

        self.assertEqual(1, len(result.steps))
        self.assertEqual(
            set(context.get_filtered_field_map('Account', lambda x: x['createable'])), 
            result.steps[0].field_scope
        )

    def test_load_load_operation_field_groups_omit_unsupported_types(self):
        context = amaxa.LoadOperation(MockSimpleSalesforce())

        ex = {
            'version': 1,
            'operation': [
                { 
                    'sobject': 'Account',
                    'field-group': 'writeable',
                    'extract': { 'all': True },
                    'input-validation': 'none'
                }
            ]
        }

        m = unittest.mock.mock_open()
        with unittest.mock.patch('builtins.open', m):
            (result, errors) = loader.load_load_operation(ex, context)

        self.assertEqual([], errors)
        self.assertIsInstance(result, amaxa.LoadOperation)

        self.assertEqual(1, len(result.steps))
        self.assertNotIn('ShippingAddress', result.steps[0].field_scope)

    def test_load_load_operation_generates_field_list(self):
        context = amaxa.LoadOperation(MockSimpleSalesforce())

        ex = {
            'version': 1,
            'operation': [
                { 
                    'sobject': 'Account',
                    'fields': [
                        'Name', 
                        'Industry'
                    ],
                    'extract': { 'all': True },
                    'input-validation': 'none'
                }
            ]
        }

        m = unittest.mock.mock_open()
        with unittest.mock.patch('builtins.open', m):
            (result, errors) = loader.load_load_operation(ex, context)
            

        self.assertEqual([], errors)
        self.assertIsInstance(result, amaxa.LoadOperation)

        self.assertEqual({'Name', 'Industry'}, result.steps[0].field_scope)

    def test_load_load_operation_respects_none_validation_option(self):
        context = amaxa.LoadOperation(MockSimpleSalesforce())

        ex = {
            'version': 1,
            'operation': [
                { 
                    'sobject': 'Account',
                    'fields': [
                        'Name', 
                        'Industry'
                    ],
                    'extract': { 'all': True },
                    'input-validation': 'none'
                }
            ]
        }

        open_mock = Mock(side_effect=[io.StringIO('Id,Name,Industry,Description'), io.StringIO()])
        with unittest.mock.patch('builtins.open', open_mock):
            (result, errors) = loader.load_load_operation(ex, context)

        self.assertEqual([], errors)
        self.assertIsInstance(result, amaxa.LoadOperation)

        self.assertEqual({'Name', 'Industry'}, result.steps[0].field_scope)

    def test_load_load_operation_validates_file_against_field_scope_excess_fields(self):
        context = amaxa.LoadOperation(MockSimpleSalesforce())

        ex = {
            'version': 1,
            'operation': [
                { 
                    'sobject': 'Account',
                    'fields': [
                        'Name', 
                        'Industry'
                    ],
                    'extract': { 'all': True }
                }
            ]
        }

        fieldnames = ['Id', 'Name', 'Industry', 'Description']
        account_mock = Mock(side_effect=[io.StringIO(','.join(fieldnames)), io.StringIO()])
        with unittest.mock.patch('builtins.open', account_mock):
            (result, errors) = loader.load_load_operation(ex, context)

        self.assertEqual(
            [
                'Input file for sObject {} does not match specified field scope.\nScope: {}\nFile Columns: {}\n'.format(
                    'Account',
                    ', '.join(sorted(['Name', 'Industry'])),
                    ', '.join(sorted(set(fieldnames) - set(['Id'])))
                )
            ],
            errors
         )
        self.assertIsNone(result)

    def test_load_load_operation_validates_file_against_field_scope_missing_fields(self):
        context = amaxa.LoadOperation(MockSimpleSalesforce())

        ex = {
            'version': 1,
            'operation': [
                { 
                    'sobject': 'Account',
                    'fields': [
                        'Name', 
                        'Industry'
                    ],
                    'extract': { 'all': True }
                }
            ]
        }

        fieldnames = ['Id', 'Name']
        account_mock = Mock(side_effect=[io.StringIO(','.join(fieldnames)), io.StringIO()])
        with unittest.mock.patch('builtins.open', account_mock):
            (result, errors) = loader.load_load_operation(ex, context)

        self.assertEqual(
            [
                'Input file for sObject {} does not match specified field scope.\nScope: {}\nFile Columns: {}\n'.format(
                    'Account',
                    ', '.join(sorted(['Name', 'Industry'])),
                    ', '.join(sorted(set(fieldnames) - set(['Id'])))
                )
            ],
            errors
         )
        self.assertIsNone(result)

    def test_load_load_operation_validates_file_against_field_group(self):
        context = amaxa.LoadOperation(MockSimpleSalesforce())

        ex = {
            'version': 1,
            'operation': [
                { 
                    'sobject': 'Account',
                    'field-group': 'writeable',
                    'extract': { 'all': True }
                }
            ]
        }

        fieldnames = ['Id', 'Name', 'Industry', 'Test__c']
        account_mock = Mock(side_effect=[io.StringIO(','.join(fieldnames)), io.StringIO()])
        with unittest.mock.patch('builtins.open', account_mock):
            (result, errors) = loader.load_load_operation(ex, context)

        self.assertIsNone(result)
        self.assertEqual(
            [
                'Input file for sObject {} contains excess columns over field group \'{}\': {}'.format(
                    'Account',
                    'writeable',
                    'Test__c'
                )
            ],
            errors
         )

    def test_load_load_operation_validates_file_against_field_group_with_strict(self):
        context = amaxa.LoadOperation(MockSimpleSalesforce())

        ex = {
            'version': 1,
            'operation': [
                { 
                    'sobject': 'Account',
                    'field-group': 'writeable',
                    'extract': { 'all': True },
                    'input-validation': 'strict'
                }
            ]
        }
        fieldnames = ['Id', 'Name']
        account_mock = Mock(side_effect=[io.StringIO(','.join(fieldnames) + '\n'), io.StringIO()])
        with unittest.mock.patch('builtins.open', account_mock):
            (result, errors) = loader.load_load_operation(ex, context)

        self.assertEqual(
            [
                'Input file for sObject {} does not match specified field scope.\nScope: {}\nFile Columns: {}\n'.format(
                    'Account',
                    ', '.join(sorted(context.get_filtered_field_map('Account', lambda f: f['createable'] and f['type'] not in ['location', 'address', 'base64']).keys())),
                    ', '.join(sorted(set(fieldnames) - set(['Id'])))
                )
            ],
            errors
         )
        self.assertIsNone(result)

    def test_load_load_operation_returns_error_unsupported_fields(self):
        context = amaxa.LoadOperation(MockSimpleSalesforce())

        ex = {
            'version': 1,
            'operation': [
                { 
                    'sobject': 'Attachment',
                    'fields': [
                        'Name',
                        'Body'
                    ],
                    'extract': { 'all': True }
                }
            ]
        }

        m = unittest.mock.mock_open()
        with unittest.mock.patch('builtins.open', m):
            (result, errors) = loader.load_load_operation(ex, context)

        self.assertEqual(['Field {}.{} is a base64 field, which is not supported.'.format('Attachment', 'Body')], errors)
        self.assertIsNone(result)
        m.assert_not_called()

    def test_load_load_operation_creates_import_mapper(self):
        context = amaxa.LoadOperation(MockSimpleSalesforce())

        ex = {
            'version': 1,
            'operation': [
                { 
                    'sobject': 'Account',
                    'fields': [
                        {
                            'field': 'Name',
                            'column': 'Account Name',
                            'transforms': ['strip', 'lowercase']
                        },
                        'Industry'
                    ],
                    'extract': { 'all': True },
                    'input-validation': 'none'
                }
            ]
        }

        m = unittest.mock.mock_open()
        with unittest.mock.patch('builtins.open', m):
            (result, errors) = loader.load_load_operation(ex, context)

        self.assertEqual([], errors)
        self.assertIsInstance(result, amaxa.LoadOperation)

        self.assertEqual({'Name', 'Industry'}, result.steps[0].field_scope)
        self.assertIn('Account', context.mappers)

        mapper = context.mappers['Account']
        self.assertEqual({'Account Name': 'Name'}, mapper.field_name_mapping)
        self.assertEqual(
            {'Name': 'university of caprica', 'Industry': 'Education'},
            mapper.transform_record({ 'Account Name': 'UNIversity of caprica  ', 'Industry': 'Education' })
        )

    def test_load_load_operation_catches_duplicate_columns(self):
        context = amaxa.LoadOperation(MockSimpleSalesforce())

        ex = {
            'version': 1,
            'operation': [
                { 
                    'sobject': 'Account',
                    'fields': [
                        {
                            'field': 'Name',
                            'column': 'Industry',
                        },
                        'Industry',
                        {
                            'field': 'Description',
                            'column': 'Industry'
                        }
                    ],
                    'extract': { 'all': True }
                }
            ]
        }

        m = unittest.mock.mock_open()
        with unittest.mock.patch('builtins.open', m):
            (result, errors) = loader.load_load_operation(ex, context)

        self.assertIsNone(result)
        self.assertEqual(
            ['Column Industry is mapped to field Account.Industry, but this column is already mapped.',
            'Column Industry is mapped to field Account.Description, but this column is already mapped.'],
            errors
        )

    def test_load_load_operation_catches_duplicate_fields(self):
        context = amaxa.LoadOperation(MockSimpleSalesforce())

        ex = {
            'version': 1,
            'operation': [
                { 
                    'sobject': 'Account',
                    'fields': [
                        {
                            'field': 'Name',
                            'column': 'Industry',
                        },
                        {
                            'field': 'Name',
                            'column': 'Name'
                        }
                    ],
                    'extract': { 'all': True }
                }
            ]
        }

        m = unittest.mock.mock_open()
        with unittest.mock.patch('builtins.open', m):
            (result, errors) = loader.load_load_operation(ex, context)

        self.assertIsNone(result)
        self.assertEqual(
            ['Field Account.Name is present more than once in the specification.'],
            errors
        )

    def test_load_load_operation_populates_lookup_behaviors(self):
        context = amaxa.LoadOperation(MockSimpleSalesforce())

        ex = {
            'version': 1,
            'operation': [
                { 
                    'sobject': 'Account',
                    'fields': [
                        'Name',
                        {
                            'field': 'ParentId',
                            'self-lookup-behavior': 'trace-none'
                        }
                    ],
                    'extract': { 'all': True },
                    'input-validation': 'none'
                },
                {
                    'sobject': 'Task',
                    'fields': [
                        {
                            'field': 'WhoId',
                            'outside-lookup-behavior': 'drop-field'
                        }
                    ],
                    'extract': { 'descendents': True },
                    'input-validation': 'none'
                }
            ]
        }

        m = unittest.mock.mock_open()
        with unittest.mock.patch('builtins.open', m):
            (result, errors) = loader.load_load_operation(ex, context)

        self.assertEqual([], errors)
        self.assertIsInstance(result, amaxa.LoadOperation)

        self.assertEqual(amaxa.SelfLookupBehavior.TRACE_NONE, result.steps[0].get_lookup_behavior_for_field('ParentId'))
        self.assertEqual(amaxa.OutsideLookupBehavior.DROP_FIELD, result.steps[1].get_lookup_behavior_for_field('WhoId'))

    def test_load_load_operation_validates_lookup_behaviors_for_self_lookups(self):
        context = amaxa.LoadOperation(MockSimpleSalesforce())

        ex = {
            'version': 1,
            'operation': [
                { 
                    'sobject': 'Account',
                    'fields': [
                        'Name',
                        {
                            'field': 'ParentId',
                            'outside-lookup-behavior': 'include'
                        }
                    ],
                    'extract': { 'all': True }
                }
            ]
        }

        m = unittest.mock.mock_open()
        with unittest.mock.patch('builtins.open', m):
            (result, errors) = loader.load_load_operation(ex, context)

        self.assertEqual(
            [
                'Lookup behavior \'{}\' specified for field {}.{} is not valid for this lookup type.'.format(
                    'include',
                    'Account',
                    'ParentId'
                )
            ],
            errors
        )
        self.assertIsNone(result)

    def test_load_load_operation_validates_lookup_behaviors_for_dependent_lookups(self):
        context = amaxa.LoadOperation(MockSimpleSalesforce())

        ex = {
            'version': 1,
            'operation': [
                { 
                    'sobject': 'Task',
                    'fields': [
                        {
                            'field': 'WhatId',
                            'self-lookup-behavior': 'trace-all'
                        }
                    ],
                    'extract': { 'all': True },
                    'input-validation': 'none'
                },
                { 
                    'sobject': 'Account',
                    'fields': [
                        'Name'
                    ],
                    'extract': { 'all': True },
                    'input-validation': 'none'
                }
            ]
        }

        m = unittest.mock.mock_open()
        with unittest.mock.patch('builtins.open', m):
            (result, errors) = loader.load_load_operation(ex, context)

        self.assertEqual(
            [
                'Lookup behavior \'{}\' specified for field {}.{} is not valid for this lookup type.'.format(
                    'trace-all',
                    'Task',
                    'WhatId'
                )
            ],
            errors
        )
        self.assertIsNone(result)

    @unittest.mock.patch('logging.getLogger')
    def test_load_load_operation_warns_lookups_other_objects(self, logger):
        context = amaxa.LoadOperation(MockSimpleSalesforce())
        amaxa_logger = Mock()
        logger.return_value=amaxa_logger

        ex = {
            'version': 1,
            'operation': [
                { 
                    'sobject': 'Account',
                    'fields': [ 'OwnerId' ],
                    'extract': { 'all': True },
                    'input-validation': 'none'
                }
            ]
        }

        m = unittest.mock.mock_open()
        with unittest.mock.patch('builtins.open', m):
            (result, errors) = loader.load_load_operation(ex, context)

        self.assertEqual([], errors)
        self.assertIsInstance(result, amaxa.LoadOperation)
        amaxa_logger.warn.assert_called_once_with(
            'Field %s.%s is a reference none of whose targets (%s) are included in the load. Reference handlers will be inactive for references to non-included sObjects.',
            'Account',
            'OwnerId',
            'User'
        )