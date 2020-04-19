from amaxa.transforms import TransformProvider
from typing import Dict


class MultiplyTransformer(TransformProvider):
    transform_name = "multiply"
    allowed_types = ["xsd:string"]

    def _get_transform(self, field_context: str, options: Dict):
        def multiply(x):
            return x * options["count"]

        return multiply

    def get_options_schema(self):
        return {"count": {"type": "integer", "required": True}}
