from abc import ABCMeta, abstractmethod
from typing import Dict

_all_transforms = None


class TransformException(Exception):
    pass


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

    def get_transform(self, field_context: Dict, options: Dict):
        self._validate_field(field_context)
        return self._get_transform(field_context, options)

    @abstractmethod
    def _get_transform(self, field_context: Dict, options: Dict):
        pass

    def _validate_field(self, field_context: Dict):
        if self.allowed_types and field_context["soapType"] not in self.allowed_types:
            raise TransformException(
                f"Transform {self.transform_name} is not available for fields of type {field_context['soapType']}."
            )

    def get_options_schema(self):
        return {}


class LowercaseTransformProvider(TransformProvider):
    transform_name = "lowercase"
    allowed_types = ["xsd:string"]

    def _get_transform(self, field_context: Dict, options: Dict):
        def lowercase(x):
            return x.lower()

        return lowercase


class UppercaseTransformProvider(TransformProvider):
    transform_name = "uppercase"
    allowed_types = ["xsd:string"]

    def _get_transform(self, field_context: Dict, options: Dict):
        def uppercase(x):
            return x.upper()

        return uppercase


class StripTransformProvider(TransformProvider):
    transform_name = "strip"
    allowed_types = ["xsd:string"]

    def _get_transform(self, field_context: Dict, options: Dict):
        def strip(x):
            return x.strip()

        return strip


class PrefixTransformProvider(TransformProvider):
    transform_name = "prefix"
    allowed_types = ["xsd:string"]

    def _get_transform(self, field_context: Dict, options: Dict):
        def prefix(x):
            return options["prefix"] + x

        return prefix

    def get_options_schema(self):
        return {"prefix": {"type": "string", "required": True}}


class SuffixTransformProvider(TransformProvider):
    transform_name = "suffix"
    allowed_types = ["xsd:string"]

    def _get_transform(self, field_context: Dict, options: Dict):
        def suffix(x):
            return x + options["suffix"]

        return suffix

    def get_options_schema(self):
        return {"suffix": {"type": "string", "required": True}}
