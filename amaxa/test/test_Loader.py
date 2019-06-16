import unittest
from unittest.mock import Mock
from io import StringIO
from ..loader import core
from ..loader.input_type import InputType


class test_Loader(unittest.TestCase):
    def test_load_returns_error_without_version(self):
        loader_test = core.Loader({}, InputType.CREDENTIALS)
        loader_test.load()
        self.assertEqual(["No version number present in schema"], loader_test.errors)

    def test_load_returns_error_with_invalid_version(self):
        loader_test = core.Loader({"version": 999}, InputType.CREDENTIALS)
        loader_test.load()
        self.assertEqual(
            ["Schema version for credentials not present or unsupported"],
            loader_test.errors,
        )

    def test_validate_schema(self):
        loader_test = core.Loader(
            {
                "version": 1,
                "operation": [
                    {
                        "sobject": "Account",
                        "fields": ["Name", "ParentId"],
                        "extract": {"all": True},
                    }
                ],
            },
            InputType.EXTRACT_OPERATION,
        )

        loader_test._validate_schema()

        self.assertEqual([], loader_test.errors)
        self.assertEqual("Account.csv", loader_test.input["operation"][0]["file"])

    def test_load_stops_after_errors(self):
        loader_test = core.Loader({"version": 999}, InputType.CREDENTIALS)
        loader_test._validate_schema = Mock()

        def cause_error():
            loader_test.errors.append("bad things happened")

        loader_test._validate = Mock(side_effect=cause_error)
        loader_test._load = Mock()

        loader_test.load()

        loader_test._validate_schema.assert_called_once()
        loader_test._validate.assert_called_once()
        loader_test._load.assert_not_called()

    def test_load_file_yaml(self):
        input_data = StringIO("test: 1")
        input_data.name = "test.yaml"
        result = core.load_file(input_data)

        self.assertEqual({"test": 1}, result)

    def test_load_file_json(self):
        input_data = StringIO('{"test": 1}')
        input_data.name = "test.json"
        result = core.load_file(input_data)

        self.assertEqual({"test": 1}, result)
