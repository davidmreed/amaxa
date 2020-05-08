from typing import List, Tuple, Union
from .amaxa import AmaxaException, FileStore, FileType, MappingMissBehavior
from .api import Connection


class MappingException(Exception):
    pass


class ObjectMapperCache:
    """Extract and cache a mapping between user-defined cache keys and sObject Ids.

    This class is used only during load operations.

    The content of the cache is two distinct types of key->value mappings.
    One maps from an Id (the lookup value in the source org) to a tuple of (sobject, key_field, key_field, ...)
    The other maps from such a tuple to the corresponding Id in the target org.
    """

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

    def get_reference_transformer(
        self, key_prefixes, miss_behavior: MappingMissBehavior, default: str = None,
    ):
        def transformer(id):
            # Make sure this is actually a reference to a mapped sObject
            # Polymorphic relationships may be only partially mapped.

            if id[:3] not in key_prefixes:
                return id

            mapped_value = self.get_cached_value(id)

            if mapped_value is None:
                if miss_behavior is MappingMissBehavior.ERROR:
                    raise MappingException(
                        f"No value available for mapped reference {id}."
                    )
                elif miss_behavior is MappingMissBehavior.DEFAULT:
                    return default

            # MappingMissBehavior.DROP is equivalent to returning None

            return mapped_value

        return transformer
