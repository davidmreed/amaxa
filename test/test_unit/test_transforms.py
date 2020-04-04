import unittest

from amaxa import transforms


class test_transforms(unittest.TestCase):
    def test_get_all_transforms(self):
        all_transforms = transforms.get_all_transforms()

        assert all_transforms == {
            "prefix": transforms.PrefixTransformProvider,
            "suffix": transforms.SuffixTransformProvider,
            "strip": transforms.StripTransformProvider,
            "lowercase": transforms.LowercaseTransformProvider,
            "uppercase": transforms.UppercaseTransformProvider,
        }

    def test_strip(self):
        transformer = transforms.StripTransformProvider().get_transform({}, {})

        assert transformer("  test  ") == "test"
        assert transformer("test") == "test"

    def test_lowercase(self):
        transformer = transforms.LowercaseTransformProvider().get_transform({}, {})

        assert transformer("TEST") == "test"
        assert transformer("test") == "test"

    def test_uppercase(self):
        transformer = transforms.UppercaseTransformProvider().get_transform({}, {})

        assert transformer("TEST") == "TEST"
        assert transformer("test") == "TEST"

    def test_prefix(self):
        transformer = transforms.PrefixTransformProvider().get_transform(
            {}, {"prefix": "foo"}
        )

        assert transformer("TEST") == "fooTEST"
        assert "prefix" in transforms.PrefixTransformProvider().get_options_schema()

    def test_suffix(self):
        transformer = transforms.SuffixTransformProvider().get_transform(
            {}, {"suffix": "foo"}
        )

        assert transformer("TEST") == "TESTfoo"
        assert "suffix" in transforms.SuffixTransformProvider().get_options_schema()
