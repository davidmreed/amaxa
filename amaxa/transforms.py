from abc import ABCMeta, abstractmethod
from typing import Dict

_all_transforms = None


def get_all_transforms():
    global _all_transforms

    def get_subclasses(cls):
        classes = []
        for subclass in cls.__subclasses__():
            classes.append(subclass)
            classes.extend(get_subclasses(subclass))

        return classes

    if _all_transforms is None:
        _all_transforms = {
            cls.transform_name: cls() for cls in get_subclasses(TransformProvider)
        }

    return _all_transforms


class TransformProvider(metaclass=ABCMeta):
    transform_name = ""
    allowed_types = []

    @abstractmethod
    def get_transform(self, field_context: str, options: Dict):
        pass

    @abstractmethod
    def get_options_schema(self):
        pass


class LowercaseTransformProvider(TransformProvider):
    transform_name = "lowercase"

    def get_transform(self, field_context: str, options: Dict):
        def lowercase(x):
            return x.lower()

        return lowercase

    def get_options_schema(self):
        return {}


class UppercaseTransformProvider(TransformProvider):
    transform_name = "uppercase"

    def get_transform(self, field_context: str, options: Dict):
        def uppercase(x):
            return x.upper()

        return uppercase

    def get_options_schema(self):
        return {}


class StripTransformProvider(TransformProvider):
    transform_name = "strip"

    def get_transform(self, field_context: str, options: Dict):
        def strip(x):
            return x.strip()

        return strip

    def get_options_schema(self):
        return {}


class PrefixTransformProvider(TransformProvider):
    transform_name = "prefix"

    def get_transform(self, field_context: str, options: Dict):
        def prefix(x):
            return options["prefix"] + x

        return prefix

    def get_options_schema(self):
        return {"prefix": {"type": "string", "required": True}}


class SuffixTransformProvider(TransformProvider):
    transform_name = "suffix"

    def get_transform(self, field_context: str, options: Dict):
        def suffix(x):
            return x + options["suffix"]

        return suffix

    def get_options_schema(self):
        return {"suffix": {"type": "string", "required": True}}
