import unittest
from unittest.mock import Mock, MagicMock, PropertyMock, patch
from .. import amaxa


class test_ExtractOperation(unittest.TestCase):
    def test_runs_all_steps(self):
        connection = Mock()
        oc = amaxa.ExtractOperation(connection)

        # pylint: disable=W0612
        for i in range(3):
            s = Mock()
            s.errors = []
            oc.add_step(s)
        
        oc.execute()

        for s in oc.steps:
            s.execute.assert_called_once_with()
            self.assertEqual(oc, s.context)
    
    def test_tracks_dependencies(self):
        connection = Mock()

        oc = amaxa.ExtractOperation(connection)

        self.assertEqual(set(), oc.get_dependencies('Account'))
        oc.add_dependency('Account', amaxa.SalesforceId('001000000000000'))
        self.assertEqual(set([amaxa.SalesforceId('001000000000000')]), oc.get_dependencies('Account'))

    def test_doesnt_add_dependency_for_extracted_record(self):
        connection = Mock()

        oc = amaxa.ExtractOperation(connection)
        oc.output_files['Account'] = Mock()

        oc.store_result('Account', { 'Id': '001000000000000', 'Name': 'Caprica Steel' })
        self.assertEqual(set(), oc.get_dependencies('Account'))
        oc.add_dependency('Account', amaxa.SalesforceId('001000000000000'))
        self.assertEqual(set(), oc.get_dependencies('Account'))

    def test_store_result_retains_ids(self):
        connection = Mock()

        oc = amaxa.ExtractOperation(connection)

        oc.output_files['Account'] = Mock()

        oc.store_result('Account', { 'Id': '001000000000000', 'Name': 'Caprica Steel' })
        self.assertEqual(set([amaxa.SalesforceId('001000000000000')]), oc.extracted_ids['Account'])

    def test_store_result_writes_records(self):
        connection = Mock()

        oc = amaxa.ExtractOperation(connection)

        account_mock = Mock()
        oc.output_files['Account'] = account_mock

        oc.store_result('Account', { 'Id': '001000000000000', 'Name': 'Caprica Steel' })
        account_mock.writerow.assert_called_once_with({ 'Id': '001000000000000', 'Name': 'Caprica Steel' })

    def test_store_result_transforms_output(self):
        connection = Mock()

        oc = amaxa.ExtractOperation(connection)

        account_mock = Mock()
        oc.output_files['Account'] = account_mock
        mapper_mock = Mock()
        mapper_mock.transform_record = Mock(return_value = { 'Id': '001000000000000', 'Name': 'Caprica City Steel' })

        oc.mappers['Account'] = mapper_mock

        oc.store_result('Account', { 'Id': '001000000000000', 'Name': 'Caprica Steel' })
        mapper_mock.transform_record.assert_called_once_with({ 'Id': '001000000000000', 'Name': 'Caprica Steel' })
        account_mock.writerow.assert_called_once_with({ 'Id': '001000000000000', 'Name': 'Caprica City Steel' })

    def test_store_result_clears_dependencies(self):
        connection = Mock()

        oc = amaxa.ExtractOperation(connection)

        oc.output_files['Account'] = Mock()
        oc.add_dependency('Account', amaxa.SalesforceId('001000000000000'))

        oc.store_result('Account', { 'Id': '001000000000000', 'Name': 'Caprica Steel' })
        self.assertEqual(set(), oc.get_dependencies('Account'))

    def test_store_result_does_not_write_duplicate_records(self):
        connection = Mock()

        oc = amaxa.ExtractOperation(connection)

        account_mock = Mock()
        oc.output_files['Account'] = account_mock

        oc.store_result('Account', { 'Id': '001000000000000', 'Name': 'Caprica Steel' })
        account_mock.writerow.assert_called_once_with({ 'Id': '001000000000000', 'Name': 'Caprica Steel' })
        account_mock.writerow.reset_mock()
        oc.store_result('Account', { 'Id': '001000000000000', 'Name': 'Caprica Steel' })
        account_mock.writerow.assert_not_called()

    def test_get_extracted_ids_returns_results(self):
        connection = Mock()

        oc = amaxa.ExtractOperation(connection)

        oc.output_files['Account'] = Mock()

        oc.store_result('Account', { 'Id': '001000000000000', 'Name': 'Caprica Steel' })
        self.assertEqual(set([amaxa.SalesforceId('001000000000000')]), oc.get_extracted_ids('Account'))

    def test_get_sobject_ids_for_reference_returns_correct_ids(self):
        connection = Mock()

        oc = amaxa.ExtractOperation(connection)

        oc.output_files['Account'] = Mock()
        oc.output_files['Contact'] = Mock()
        oc.output_files['Opportunity'] = Mock()
        oc.get_field_map = Mock(return_value={ 'Lookup__c': { 'referenceTo': ['Account', 'Contact'] }})

        oc.store_result('Account', { 'Id': '001000000000000', 'Name': 'University of Caprica' })
        oc.store_result('Contact', { 'Id': '003000000000000', 'Name': 'Gaius Baltar' })
        oc.store_result('Opportunity', { 'Id': '006000000000000', 'Name': 'Defense Mainframe' })

        self.assertEqual(set([amaxa.SalesforceId('001000000000000'), amaxa.SalesforceId('003000000000000')]),
                         oc.get_sobject_ids_for_reference('Account', 'Lookup__c'))

    def test_close_files_closes_all_handles(self):
        connection = Mock()

        op = amaxa.ExtractOperation(connection)
        op.output_file_handles = {
            'Account': Mock(),
            'Contact': Mock()
        }

        op.close_files()

        for f in op.output_file_handles.values():
            f.close.assert_called_once_with()

    def test_execute_calls_close_files_on_error(self):
        connection = Mock()
        first_step = Mock()
        first_step.errors = ['err']

        op = amaxa.ExtractOperation(connection)
        op.close_files = Mock()

        op.add_step(first_step)

        self.assertEqual(-1, op.execute())
        op.close_files.assert_called_once_with()

    def test_execute_calls_close_files_on_success(self):
        connection = Mock()
        first_step = Mock()
        first_step.errors = []

        op = amaxa.ExtractOperation(connection)
        op.close_files = Mock()

        op.add_step(first_step)

        self.assertEqual(0, op.execute())
        op.close_files.assert_called_once_with()