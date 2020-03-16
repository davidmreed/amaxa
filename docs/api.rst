API Usage
---------

Amaxa uses both the REST and Bulk APIs to do its work.

When extracting, it consumes one Bulk API job for each sObject with ``extract`` set to ``all`` or ``query``, plus approximately one API call (to the REST API) per 200 records that are extracted by Id due to dependencies or ``extract`` set to ``descendents``.

When loading, Amaxa uses one Bulk API batch for each batch of records of each sObject, plus one Bulk API batch for each batch records of each sObject that has self- or dependent lookups. The batch size defaults to 10,000 and is configurable. Only records requiring dependent processing are included in the second phase.

A small number of additional API calls are used on each operation to obtain schema information for the org.
