from abc import ABCMeta, abstractmethod
from typing import Dict


def get_all_transforms():
    def get_subclasses(cls):
        classes = []
        for subclass in cls.__subclasses__():
            classes.append(subclass)
            classes.extend(get_subclasses(subclass))

        return classes

    return {cls.transform_name: cls for cls in get_subclasses(TransformProvider)}


class TransformProvider(metaclass=ABCMeta):
    transform_name = ""
    allowed_types = []

    @abstractmethod
    def get_transform(self, field_context: Dict, options: Dict):
        pass

    @abstractmethod
    def get_options_schema(self):
        pass


class LowercaseTransformProvider(TransformProvider):
    transform_name = "lowercase"
    allowed_types = ["xsd:string"]

    def get_transform(self, field_context: Dict, options: Dict):
        def lowercase(x):
            return x.lower()

        return lowercase

    def get_options_schema(self):
        return {}


class UppercaseTransformProvider(TransformProvider):
    transform_name = "uppercase"
    allowed_types = ["xsd:string"]

    def get_transform(self, field_context: Dict, options: Dict):
        def uppercase(x):
            return x.upper()

        return uppercase

    def get_options_schema(self):
        return {}


class StripTransformProvider(TransformProvider):
    transform_name = "strip"
    allowed_types = ["xsd:string"]

    def get_transform(self, field_context: Dict, options: Dict):
        def strip(x):
            return x.strip()

        return strip

    def get_options_schema(self):
        return {}


class PrefixTransformProvider(TransformProvider):
    transform_name = "prefix"
    allowed_types = ["xsd:string"]

    def get_transform(self, field_context: Dict, options: Dict):
        def prefix(x):
            return options["prefix"] + x

        return prefix

    def get_options_schema(self):
        return {"prefix": {"type": "string", "required": True}}


class SuffixTransformProvider(TransformProvider):
    transform_name = "suffix"
    allowed_types = ["xsd:string"]

    def get_transform(self, field_context: Dict, options: Dict):
        def suffix(x):
            return x + options["suffix"]

        return suffix

    def get_options_schema(self):
        return {"suffix": {"type": "string", "required": True}}
