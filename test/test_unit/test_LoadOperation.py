import unittest
from unittest.mock import Mock

import amaxa
from amaxa import constants

from .MockFileStore import MockFileStore


class test_LoadOperation(unittest.TestCase):
    def test_maps_record_ids(self):
        connection = Mock()
        op = amaxa.LoadOperation(connection)
        op.file_store = MockFileStore()

        op.register_new_id(
            "Account",
            amaxa.SalesforceId("001000000000000"),
            amaxa.SalesforceId("001000000000001"),
        )

        self.assertEqual(
            amaxa.SalesforceId("001000000000001"),
            op.get_new_id(amaxa.SalesforceId("001000000000000")),
        )

    def test_register_new_id_writes_result_entries(self):
        connection = Mock()
        op = amaxa.LoadOperation(connection)
        op.file_store = MockFileStore()

        op.register_new_id(
            "Account",
            amaxa.SalesforceId("001000000000000"),
            amaxa.SalesforceId("001000000000001"),
        )

        op.file_store.get_csv(
            "Account", amaxa.FileType.RESULT
        ).writerow.assert_called_once_with(
            {
                constants.ORIGINAL_ID: str(amaxa.SalesforceId("001000000000000")),
                constants.NEW_ID: str(amaxa.SalesforceId("001000000000001")),
            }
        )

    def test_execute_runs_all_passes(self):
        connection = Mock()
        first_step = Mock(sobjectname="Account")
        second_step = Mock(sobjectname="Contact")

        op = amaxa.LoadOperation(connection)
        op.file_store = MockFileStore()

        op.add_step(first_step)
        op.add_step(second_step)

        self.assertEqual(0, op.execute())

        first_step.execute.assert_called_once_with()
        first_step.execute_dependent_updates.assert_called_once_with()

        second_step.execute.assert_called_once_with()
        second_step.execute_dependent_updates.assert_called_once_with()

    def test_execute_stops_after_first_error_in_step_execute(self):
        connection = Mock()
        op = amaxa.LoadOperation(connection)
        op.file_store = MockFileStore()

        first_step = Mock(sobjectname="Account")
        second_step = Mock(sobjectname="Contact")
        first_step.execute.side_effect = lambda: op.register_error(
            "Account", "001000000000000", "err"
        )

        op.add_step(first_step)
        op.add_step(second_step)

        self.assertEqual(-1, op.execute())

        first_step.execute.assert_called_once_with()
        first_step.execute_dependent_updates.assert_not_called()

        second_step.execute.assert_not_called()
        second_step.execute_dependent_updates.assert_not_called()

    def test_execute_stops_after_first_error_in_step_execute_dependent_updates(self):
        connection = Mock()
        op = amaxa.LoadOperation(connection)
        op.file_store = MockFileStore()

        first_step = Mock(sobjectname="Account")
        second_step = Mock(sobjectname="Contact")
        first_step.execute_dependent_updates.side_effect = lambda: op.register_error(
            "Account", "001000000000000", "err"
        )

        op.add_step(first_step)
        op.add_step(second_step)

        self.assertEqual(-1, op.execute())

        first_step.execute.assert_called_once_with()
        first_step.execute_dependent_updates.assert_called_once_with()

        second_step.execute.assert_called_once_with()
        second_step.execute_dependent_updates.assert_not_called()

    def test_register_error_logs_to_result_file(self):
        connection = Mock()
        first_step = Mock()
        first_step.sobjectname = "Account"

        op = amaxa.LoadOperation(connection)
        op.file_store = MockFileStore()
        op.add_step(first_step)
        first_step.execute.side_effect = lambda: op.register_error(
            "Account", "001000000000000", "err"
        )

        self.assertEqual(-1, op.execute())
        op.file_store.get_csv(
            "Account", amaxa.FileType.RESULT
        ).writerow.assert_called_once_with(
            {constants.ORIGINAL_ID: "001000000000000", constants.ERROR: "err"}
        )

    def test_execute_resumes_with_dependent_updates_if_stage_set(self):
        connection = Mock()
        first_step = Mock(sobjectname="Account")
        second_step = Mock(sobjectname="Contact")

        op = amaxa.LoadOperation(connection)
        op.file_store = MockFileStore()
        op.stage = amaxa.LoadStage.DEPENDENTS

        op.add_step(first_step)
        op.add_step(second_step)

        self.assertEqual(0, op.execute())

        first_step.execute.assert_not_called()
        second_step.execute.assert_not_called()

        first_step.execute_dependent_updates.assert_called_once_with()
        second_step.execute_dependent_updates.assert_called_once_with()
