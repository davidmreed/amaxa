from typing import List, Tuple, Union

from .amaxa import (
    ExtractOperation,
    ExtractionStep,
    ExtractionScope,
    FileType,
    MappingMissBehavior,
)


class MappingException(Exception):
    pass


class ObjectMapperCache(ExtractOperation):
    """Extract and cache a mapping between user-defined cache keys and sObject Ids.

    This class is used only during load operations.

    The content of the cache is two distinct types of key->value mappings.
    One maps from an Id (the lookup value in the source org) to a tuple of (sobject, key_field, key_field, ...)
    The other maps from such a tuple to the corresponding Id in the target org.
    """

    def __init__(self, connection, file_store):
        super().__init__(connection)
        self.file_store = file_store
        self._cache = {}
        self._cache_schema = {}
        self._key_prefixes = set()
        self._behaviors = {}
        self._defaults = {}

    def add_cached_sobject(
        self,
        sobject: str,
        key_fields: List[str],
        miss_behavior: MappingMissBehavior,
        default: Tuple[str] = None,
    ):
        """Add a target sObject to the mapping cache. Key the sObject on the
        combination keys in key_fields."""

        self._cache_schema[sobject] = key_fields
        self._behaviors[sobject] = miss_behavior
        self._defaults[sobject] = default
        self.add_step(
            ExtractionStep(sobject, ExtractionScope.ALL_RECORDS, ["Id"] + key_fields)
        )

    def _store_cache_key(
        self, key: Union[Tuple[str], str], value: Union[Tuple[str], str]
    ):
        """Add a key to the cache."""
        if key in self._cache:
            err = f"The mapped object key {key} is duplicated."
            self.logger.error(err)
            self.errors.append(err)  # FIXME: We don't have an `errors` member. Raise?

        self._cache[key] = value

    def _read_mapping_files(self):
        """Load data from *.mapping.csv files into the cache, keying from original Id
        to a tuple of (sobject, key_field_1, key_field_2, ...)."""
        for sobject in self._cache_schema:
            csv = self.file_store.get_csv(sobject, FileType.INPUT)
            schema = self._cache_schema[sobject]

            for r in csv:
                self._store_cache_key(
                    r["Id"], tuple([sobject] + [r[key] for key in schema])
                )
                self._key_prefixes.add(r["Id"][:3])

    def store_result(self, sobjectname, record):
        schema = self._cache_schema[sobjectname]
        cache_key = tuple([sobjectname] + [record[f] for f in schema])
        self._store_cache_key(cache_key, record["Id"])

    def initialize(self):
        super().initialize()
        self._read_mapping_files()

    def execute(self):
        """Extract data from the target org for the entire table of each mapped sObject.
        Populate the cache with keys formed from key_fields and values equal to Salesforce Ids."""

        self.logger.info("Extracting mapped sObjects to cache")

        retval = super().execute()

        if not retval:
            return self._check_default_values()

        return retval

    def _check_default_values(self):
        errors = False
        for sobject in self._defaults:
            if (sobject, *self._defaults[sobject]) not in self._cache:
                self.logger.error(
                    f"No record present for default mapping target {self._defaults[sobject]} for sObject {sobject}"
                )
                errors = True

        return -1 if errors else 0

    def get_cached_value(self, cache_key: Union[str, Tuple[str]]):
        if cache_key is None:
            return None

        return self._cache.get(self._cache.get(cache_key))

    def get_reference_transformer(self):
        def transformer(id: str):
            # Make sure this is actually a reference to a mapped sObject
            # Polymorphic relationships may be only partially mapped.

            # Important: the Id we're receiving has a key prefix from its source
            # org, which may not be the same in our target org. That's why we
            # store key prefixes from our loaded mapping data, so we know which
            # Ids to map for polymorphic lookups.
            if id[:3] not in self._key_prefixes:
                return id

            mapped_value = self.get_cached_value(id)

            if mapped_value is None:
                sobject = self.connection.get_sobject_name_for_id(id)
                miss_behavior = self._behaviors[sobject]

                if miss_behavior is MappingMissBehavior.ERROR:
                    raise MappingException(
                        f"No value available for mapped reference {id}."
                    )
                elif miss_behavior is MappingMissBehavior.DEFAULT:
                    return self.get_cached_value(
                        (sobject, *self._defaults.get(sobject))
                    )

            # MappingMissBehavior.DROP is equivalent to returning None

            return mapped_value

        return transformer
