import pytest
import unittest

from amaxa import transforms

TEST_FIELD = {"name": "Test__c", "soapType": "xsd:string"}

TEST_FIELD_FAILURE = {"name": "ParentId", "soapType": "tns:Id"}


class test_transforms(unittest.TestCase):
    def test_get_all_transforms(self):
        all_transforms = transforms.get_all_transforms()

        # If integration tests have been run first, we may have an example transform present.
        transform_keys = set(all_transforms.keys())
        if "multiply" in transform_keys:
            transform_keys.remove("multiply")
        assert transform_keys == {
            "prefix",
            "suffix",
            "strip",
            "lowercase",
            "uppercase",
        }
        assert isinstance(all_transforms["prefix"], transforms.PrefixTransformProvider)
        assert isinstance(all_transforms["suffix"], transforms.SuffixTransformProvider)
        assert isinstance(all_transforms["strip"], transforms.StripTransformProvider)
        assert isinstance(
            all_transforms["lowercase"], transforms.LowercaseTransformProvider
        )
        assert isinstance(
            all_transforms["uppercase"], transforms.UppercaseTransformProvider
        )

    def test_strip(self):
        transformer = transforms.StripTransformProvider().get_transform(TEST_FIELD, {})

        assert transformer("  test  ") == "test"
        assert transformer("test") == "test"

    def test_lowercase(self):
        transformer = transforms.LowercaseTransformProvider().get_transform(
            TEST_FIELD, {}
        )

        assert transformer("TEST") == "test"
        assert transformer("test") == "test"

    def test_uppercase(self):
        transformer = transforms.UppercaseTransformProvider().get_transform(
            TEST_FIELD, {}
        )

        assert transformer("TEST") == "TEST"
        assert transformer("test") == "TEST"

    def test_prefix(self):
        transformer = transforms.PrefixTransformProvider().get_transform(
            TEST_FIELD, {"prefix": "foo"}
        )

        assert transformer("TEST") == "fooTEST"
        assert "prefix" in transforms.PrefixTransformProvider().get_options_schema()

    def test_suffix(self):
        transformer = transforms.SuffixTransformProvider().get_transform(
            TEST_FIELD, {"suffix": "foo"}
        )

        assert transformer("TEST") == "TESTfoo"
        assert "suffix" in transforms.SuffixTransformProvider().get_options_schema()

    def test_field_type_exception(self):
        with pytest.raises(transforms.TransformException):
            transforms.SuffixTransformProvider().get_transform(
                TEST_FIELD_FAILURE, {"suffix": "foo"}
            )
