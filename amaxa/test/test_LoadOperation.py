import unittest
from unittest.mock import Mock, MagicMock, PropertyMock, patch
from .. import amaxa
from .. import constants


class test_LoadOperation(unittest.TestCase):
    def test_stores_input_files(self):
        connection = Mock()
        op = amaxa.LoadOperation(connection)

        op.set_input_file('Account', 'a')
        self.assertEqual('a', op.get_input_file('Account'))

    def test_stores_result_files(self):
        connection = Mock()
        op = amaxa.LoadOperation(connection)

        op.set_result_file('Account', 'a', Mock())
        self.assertEqual('a', op.get_result_file('Account'))

    def test_maps_record_ids(self):
        connection = Mock()
        op = amaxa.LoadOperation(connection)

        op.register_new_id('Account', amaxa.SalesforceId('001000000000000'), amaxa.SalesforceId('001000000000001'))

        self.assertEqual(amaxa.SalesforceId('001000000000001'), op.get_new_id(amaxa.SalesforceId('001000000000000')))

    def test_writes_result_entries(self):
        connection = Mock()
        op = amaxa.LoadOperation(connection)
        result_mock = Mock()
        op.set_result_file('Account', result_mock, Mock())

        op.register_new_id('Account', amaxa.SalesforceId('001000000000000'), amaxa.SalesforceId('001000000000001'))

        result_mock.writerow.assert_called_once_with(
            {
                constants.ORIGINAL_ID: str(amaxa.SalesforceId('001000000000000')),
                constants.NEW_ID: str(amaxa.SalesforceId('001000000000001'))
            }
        )

    def test_execute_runs_all_passes(self):
        connection = Mock()
        first_step = Mock()
        second_step = Mock()
        first_step.errors = second_step.errors = {}

        op = amaxa.LoadOperation(connection)

        op.add_step(first_step)
        op.add_step(second_step)

        self.assertEqual(0, op.execute())

        first_step.execute.assert_called_once_with()
        first_step.execute_dependent_updates.assert_called_once_with()

        second_step.execute.assert_called_once_with()
        second_step.execute_dependent_updates.assert_called_once_with()

    def test_execute_stops_after_first_error_in_step_execute(self):
        connection = Mock()
        first_step = Mock()
        first_step.errors = {'001000000000000': 'err'}
        second_step = Mock()
        second_step.errors = {}

        op = amaxa.LoadOperation(connection)

        op.add_step(first_step)
        op.add_step(second_step)

        self.assertEqual(-1, op.execute())

        first_step.execute.assert_called_once_with()
        first_step.execute_dependent_updates.assert_not_called()

        second_step.execute.assert_not_called()
        second_step.execute_dependent_updates.assert_not_called()

    def test_execute_stops_after_first_error_in_step_execute_dependent_updates(self):
        def side_effect():
            first_step.errors = {'001000000000000': 'err'}
        
        connection = Mock()
        first_step = Mock()
        first_step.errors = {}
        first_step.execute_dependent_updates = Mock(side_effect=side_effect)
        second_step = Mock()
        second_step.errors = {}

        op = amaxa.LoadOperation(connection)

        op.add_step(first_step)
        op.add_step(second_step)

        self.assertEqual(-1, op.execute())

        first_step.execute.assert_called_once_with()
        first_step.execute_dependent_updates.assert_called_once_with()

        second_step.execute.assert_called_once_with()
        second_step.execute_dependent_updates.assert_not_called()

    def test_execute_calls_write_errors(self):
        connection = Mock()
        first_step = Mock()
        first_step.errors = {'001000000000000': 'err'}

        op = amaxa.LoadOperation(connection)
        op.write_errors = Mock()

        op.add_step(first_step)
        
        self.assertEqual(-1, op.execute())

        first_step.execute.assert_called_once_with()
        op.write_errors.assert_called_once_with(first_step)

    def test_write_errors_logs_to_result_file(self):
        connection = Mock()
        first_step = Mock()
        first_step.sobjectname = 'Account'
        first_step.errors = {'001000000000000': 'err'}

        op = amaxa.LoadOperation(connection)
        op.result_files = { 'Account': Mock() }

        op.add_step(first_step)
        
        self.assertEqual(-1, op.execute())
        op.result_files['Account'].writerow.assert_called_once_with(
            {
                constants.ORIGINAL_ID: '001000000000000',
                constants.ERROR: 'err'
            }
        )

    def test_close_files_closes_all_handles(self):
        connection = Mock()

        op = amaxa.LoadOperation(connection)
        op.result_file_handles = {
            'Account': Mock(),
            'Contact': Mock()
        }

        op.close_files()

        for f in op.result_file_handles.values():
            f.close.assert_called_once_with()

    def test_execute_calls_close_files_on_error(self):
        connection = Mock()
        first_step = Mock()
        first_step.errors = {'001000000000000': 'err'}

        op = amaxa.LoadOperation(connection)
        op.close_files = Mock()

        op.add_step(first_step)

        self.assertEqual(-1, op.execute())
        op.close_files.assert_called_once_with()

    def test_execute_calls_close_files_on_success(self):
        connection = Mock()
        first_step = Mock()
        first_step.errors = {}

        op = amaxa.LoadOperation(connection)
        op.close_files = Mock()

        op.add_step(first_step)

        self.assertEqual(0, op.execute())
        op.close_files.assert_called_once_with()