import unittest
from unittest.mock import Mock

import amaxa

from .MockConnection import MockConnection


class ConcreteOperation(amaxa.Operation):
    def execute(self):
        pass


class test_Operation(unittest.TestCase):
    def test_stores_steps(self):
        connection = Mock()
        oc = ConcreteOperation(connection)

        step = Mock()
        oc.add_step(step)

        self.assertEqual([step], oc.steps)
        self.assertEqual(oc, step.context)

    def test_filters_field_maps(self):
        connection = MockConnection()

        oc = ConcreteOperation(connection)

        retval = oc.get_filtered_field_map("Account", lambda f: f["name"] == "Id")
        self.assertEqual(1, len(retval))
        self.assertEqual("Id", retval["Id"]["name"])

    def test_run_calls_initialize_and_execute(self):
        connection = Mock()
        op = ConcreteOperation(connection)
        op.initialize = Mock()
        op.execute = Mock(return_value=0)
        op.file_store = Mock()

        self.assertEqual(0, op.run())
        op.initialize.assert_called_once_with()
        op.execute.assert_called_once_with()
        op.file_store.close.assert_called_once_with()

    def test_run_logs_exceptions(self):
        connection = Mock()
        op = ConcreteOperation(connection)
        op.initialize = Mock()
        op.execute = Mock(side_effect=amaxa.AmaxaException("Test"))
        op.logger = Mock()
        op.file_store = Mock()

        self.assertEqual(-1, op.run())

        op.logger.error.assert_called_once_with("Unexpected exception Test occurred.")
        op.file_store.close.assert_called_once_with()
