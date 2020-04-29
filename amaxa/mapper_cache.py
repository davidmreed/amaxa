from typing import List, Tuple, Union
from .amaxa import AmaxaException, FileStore, FileType, StringEnum
from .api import Connection


class MappingMissBehavior(StringEnum):
    ERROR = "error"
    DROP = "drop"
    DEFAULT = "default"


class ObjectMapperCache:
    """Extract and cache a mapping between user-defined cache keys and sObject Ids.

    This class is used only during load operations."""

    def __init__(self):
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

    def _store_cache_key(
        self, key: Union[Tuple[str], str], value: Union[Tuple[str], str]
    ):
        """Add a key to the cache. Throw AmaxaException if already mapped."""
        if key in self._cache:
            raise AmaxaException(f"The cache key {key} is duplicated.")

        self._cache[key] = value

    def _read_mapping_files(self, file_store: FileStore):
        """Load data from *.mapping.csv files into the cache, keying from original Id
        to a tuple of (sobject, key_field_1, key_field_2, ...)."""
        for sobject in self._cache_schema:
            csv = file_store.get_csv(sobject, FileType.INPUT)
            schema = self._cache_schema[sobject]

            for r in csv:
                self._store_cache_key(
                    r["Id"], tuple([sobject] + (r[key] for key in schema))
                )

    def _extract_records(self, conn: Connection):
        """Extract data from the target org into the cache, keying from a tuple of
        (sobject, key_field_1, key_field_2, ...) to new Id."""
        for sobject, schema in self._cache_schema.items():
            self._cache[sobject] = {}
            fields = ", ".join(schema)
            for result in self.conn.bulk_api_query(
                sobject, f"SELECT {fields} FROM {self.sobject}"
            ):
                cache_key = tuple([sobject] + (result[f] for f in schema))
                self._store_cache_key(cache_key, result["Id"])

    def populate_cache(self, conn: Connection, file_store: FileStore):
        """Extract data from the target org for the entire table of each mapped sObject.
        Populate the cache with keys formed from key_fields and values equal to Salesforce Ids."""
        if self._cache:
            return

        self._read_mapping_files(file_store)
        self._extract_records(conn)

    def get_cached_value(self, cache_key: Tuple[str]):
        return self._cache.get(self._cache.get(cache_key))

    def get_cached_sobjects(self):
        return self._cache_schema.keys()


def transform_reference(cache: ObjectMapperCache, target_sobject: str, value: str):
    """After partial application with functools.partial, map references in a loadable
    sObject record to the Ids of their target objects, based upon key_fields."""

    return cache.get_cached_value((target_sobject, value,))


def transform_record_type_reference(
    cache: ObjectMapperCache, source_sobject: str, value: str
):
    """After partial application with functools.partial, map Record Type references
    in a loadable sObject record to the corresponding Ids based on Developer Name."""
    return cache.get_cached_value(("RecordType", source_sobject, value,))
