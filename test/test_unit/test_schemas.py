from unittest.mock import Mock

from amaxa.loader import schemas


def test_coerce_transform():
    assert schemas._coerce_transform("strip") == {"name": "strip", "options": {}}
    assert schemas._coerce_transform({"name": "strip", "options": {}}) == {
        "name": "strip",
        "options": {},
    }


def test_validate_import_module__failure():
    error = Mock()
    field = Mock()

    schemas._validate_import_module(field, "///__none__", error)
    error.assert_called_once_with(field, "Unable to import module ///__none__")


def test_validate_import_module__success():
    error = Mock()
    field = Mock()

    schemas._validate_import_module(field, "os", error)
    error.assert_not_called()


def test_validate_transform_options():
    error = Mock()
    field = Mock()

    schemas._validate_transform_options(
        field, {"name": "suffix", "options": {"suffix": "test"}}, error
    )

    error.assert_not_called()


def test_validate_transform_options__missing_transform():
    error = Mock()
    field = Mock()

    schemas._validate_transform_options(
        field, {"name": "__bogus", "options": {}}, error
    )
    error.assert_called_once_with(field, "The transform __bogus does not exist.")


def test_validate_transform_options__invalid_options():
    error = Mock()
    field = Mock()

    schemas._validate_transform_options(field, {"name": "suffix", "options": {}}, error)
    error.assert_called_once_with(
        field, "The options schema for transform suffix failed to validate: suffix",
    )
