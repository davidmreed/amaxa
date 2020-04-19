from typing import List, Tuple
from .amaxa import AmaxaException
from .api import Connection


class ObjectMapperCache:
    """Extract and cache a mapping between user-defined cache keys and sObject Ids.

    This class is used only during load operations. During extractions, cache keys are
    retrieved via relationship queries."""

    def __init__(self, conn: Connection):
        self.conn = conn
        self._cache = None
        self._cache_schema = {}

    def add_cached_sobject(self, sobject: str, key_fields: List[str]):
        """Add a target sObject to the mapping cache. Key the sObject on the
        combination keys in key_fields. Must be called before populate_cache()
        or an exception will be thrown."""
        if self._cache:
            raise AmaxaException(f"Cache has already been populated")

        if sobject not in self._cache_schema:
            self._cache_schema[sobject] = key_fields
        elif self._cache_schema[sobject] != key_fields:
            raise AmaxaException(
                f"sObject {sobject} cannot be mapped to multiple key fields."
            )

    def populate_cache(self):
        """Extract data from the target org for the entire table of each mapped sObject.
        Populate the cache with keys formed from key_fields and values equal to Salesforce Ids."""
        if self._cache:
            return

        for sobject, schema in self._cache_schema.items():
            self._cache[sobject] = {}
            fields = ", ".join(schema)
            for result in self.conn.bulk_api_query(
                self.sobject, f"SELECT {fields} FROM {self.sobject}"
            ):
                cache_key = tuple(result[f] for f in schema)
                if cache_key in self.cache[sobject]:
                    raise AmaxaException(
                        f"There are duplicate records in the sObject {sobject} for key fields {fields}"
                    )

                self._cache[sobject][cache_key] = result["Id"]

    def get_cached_value(self, sobject: str, values: Tuple[str]):
        return self._cache[sobject].get(values)

    def get_cached_sobjects(self):
        return self._cache.keys()


def transform_reference(cache: ObjectMapperCache, target_sobject: str, value: str):
    """After partial application with functools.partial, map references in a loadable
    sObject record to the Ids of their target objects, based upon key_fields."""

    return cache.get_cached_value(target_sobject, (value,))


def transform_record_type_reference(
    cache: ObjectMapperCache, source_sobject: str, value: str
):
    """After partial application with functools.partial, map Record Type references
    in a loadable sObject record to the corresponding Ids based on Developer Name."""
    return cache.get_cached_value("RecordType", (source_sobject, value))
