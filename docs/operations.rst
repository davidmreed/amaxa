Defining Operations
-------------------

.. note:: Amaxa configuration files are versioned and are validated at runtime. This documentation is for the current version of the credential file, version 2. Upgrade existing projects with version 1 files to make use of new features.

Operations run with Amaxa are established by an operation definition file written in either JSON or YAML. The operation definition specifies which sObjects to extract or load in which order, and which fields on each object are desired. Amaxa handles tracing relationships between top-level objects and their children and extracts a set of CSV files to produce a complete, internally consistent data set.

Here's an example of an Amaxa operation definition in YAML.

.. code-block:: yaml

    version: 2
    options:
        bulk-api-batch-size: 10000
        bulk-api-timeout: 600
        bulk-api-poll-interval: 10
    operation:
        -
            sobject: Account
            options:
                bulk-api-batch-size: 5000
            fields:
                - Name
                - Id
                - ParentId
                -
                    field: Description
                    column: Desc
                    transforms:
                        - strip
                        - lowercase
            extract:
                query: 'Industry = "Non-Profit"'
        -
            sobject: Contact
            file: 'Contacts-New.csv'
            field-group: readable
            outside-reference-behavior: drop-field
            extract:
                descendents: True
        -
            sobject: Opportunity
            field-group: readable
            extract:
                descendents: True

The meat of the definition is list of sObjects under the ``operation`` key. We list objects in order of their extraction or load. The order is important, because it plays into how Amaxa will locate, extract, and load relationships between the objects. Typically, you should start an operation definition with the highest-level object you want to extract or load. This may be ``Account``, for example, or if loading a custom object with children, the parent object. Then, we list child objects and other dependencies in order below it. Objects listed later in the list may be extracted based on lookup or master-detail relationships to objects higher in the list. (More details on object-sequence patterns can be found below.)

For each object we choose to extract, we answer a few main questions.

Which records do we want to extract?
************************************

Record selection is specified with the ``extract:`` key. We can specify several different types of record-level extraction mechanics. The ``extract`` key is ignored during load operations.

.. code-block:: yaml

    query: 'Industry = "Non-Profit"'

The ``query`` type of extraction pulls records that match a SOQL ``WHERE`` clause that you supply. (Do not include the ``WHERE`` keyword).

.. code-block:: yaml

    descendents: True

The ``descendents`` type of extraction pulls records that have a lookup or master-detail relationship to any object higher in the operation definition. This relationship can be any field that is included in the selected fields for the object. For example, if extracting ``Account`` followed by ``Contact``, with ``descendents: True`` specified, Amaxa will pull Contacts associated to all extracted Accounts via *any lookup field from ``Contact`` to ``Account`` that is included in the operation*. This could, for example, include ``AccountId`` as well as some custom field ``Other_Account__c``. If another object were above ``Contact`` in the operation and ``Contact`` has a relationship to that object, Amaxa would also pull ``Contact`` records associated to extracted records for that object.

.. code-block:: yaml

    ids:
        - 003000000000001
        - 003000000000002
        - 003000000000003

The ``ids`` type of extraction pulls specific records by ``Id``, supplied in a list.

All types of extraction also retrieve *dependent relationships*. When an sObject higher in the operation has a relationship to an sObject lower in the operation, the Ids of referenced objects are recorded and extracted later in the process. For example, if an included field on ``Account`` is a relationship ``Primary_Contact__c`` to ``Contact``, but ``Account`` is extracted first, Amaxa will ensure that all referenced records are extracted during the ``Contact`` step.

The combination of dependent and descendent relationship tracing helps ensure that Amaxa extracts and loads an internally consistent slice of your org's data based upon the operation definition you provide.

Which fields do we want to extract or load?
*******************************************

This is specified with the ``fields`` or ``field-group`` keys.

The easiest way to select fields is to specify ``field-group: [smart|readable|writeable]``. This instructs Amaxa to automatically determine which fields to extract based on access level: ``readable`` is all accessible fields, ``writeable`` all createable and updateable fields, and ``smart`` will automatically select ``readable`` for extract operations and ``writeable`` for loads. The use of field groups streamlines the configuration file, but is most suitable for extract and load operations performed on the same org or related orgs, like sandboxes derived from the same production org. This is because Amaxa will extract references to, for example, Record Types and Users whose Ids may differ across unrelated orgs.

If you're moving data between unrelated orgs or wish to specify the exact field set for each sObject, use the ``fields`` key. The value of this key is a list whose elements are either the API name of a single field or a map specifying how to load, extract, and transform the data.

.. code-block:: yaml

    fields:
        - Name
        - Industry

is an example of a simple field specification.

.. code-block:: yaml

    fields:
        -
            field: Description
            column: Desc
            transforms:
                - strip
                - lowercase

would extract the ``Description`` field, name the CSV column ``Desc``, and apply the transformations ``strip`` (remove leading and trailing whitespace) and ``lowercase`` (convert text to lower case) on extracted data. On load, Amaxa would look for a CSV column ``Desc``, map it to the ``Description`` field, and apply the same transformations to inbound data.

``fields`` and ``field-group`` can be combined if you wish to customize the behavior of ``field-group`` by adding column mappings or specifying additional fields that don't exist in the group. Additionally, the ``exclude-fields`` key can be used to suppress fields you don't want that might otherwise be included by the chosen ``field-group``.

.. code-block:: yaml

    field-group: smart
    fields:
        -
            field: Description
            column: Desc
            transforms:
                - strip
                - lowercase
    exclude-fields:
        - OwnerId

Where is the data going to or coming from?
******************************************

The ``file`` key for each sObject specifies a CSV file. This is the input data for a load operation, or the output data for an extraction. Amaxa will specify ``sObjectName.csv`` if the key is not provided.

For loads, Amaxa will also use a ``result-file`` key, which specifies the location for the output Id map and error file. If not supplied, Amaxa will use ``sObjectName-results.csv``. The results file has three columns: ``"Original Id"``, ``"New Id"``, and ``"Error"``.

Object sequencing in an operation
*********************************

As shown in the example above, to extract or load a parent object and its children, list the parent first, followed by the child, and specify ``extract: descendents: True`` for the child. If the parent is itself a child of a higher-level parent, you can use ``descendents`` there too - just make sure your operation definition starts with at least one object that is configured with ``extract: all: True``, ``extract: ids: <list>``, or ``extract: query: <where clause>`` so that Amaxa has a designated record set with which to begin.

.. code-block:: yaml

    operation:
        -
            sobject: Account
            field-group: readable
            extract:
                query: 'Industry = "Non-Profit"'
        -
            sobject: Contact
            field-group: readable
            extract:
                descendents: True

There are other patterns that can in specific situations be useful. It's permissible, for example, to start with the child object, and allow Amaxa to pull parent objects as dependencies. (Note that this approach may be somewhat less performant upon re-loading data, as Amaxa must run two passes to populate all of the lookup fields)

.. code-block:: yaml

    operation:
        -
            sobject: Contact
            field-group: readable
            extract:
                query: 'Email != null'
        -
            sobject: Account
            field-group: readable
            extract:
                descendents: True

In this pattern, Amaxa will not find any descendents for ``Account`` (unless ``Account`` itself has a lookup to ``Contact``), but it will pull the parent Accounts of all of the extracted Contacts as dependencies, because the ``AccountId`` field is included in the operation.

Junction objects may be selected in several different ways. Suppose we have objects A and B, joined with a junction object C.

1. **We want to extract specific A records, with all of their C junctions and the B records associated to them.**
    We specify A first, then C, then B. C and B have ``descendents: True`` set under ``extract:``.
2. **We want to extract all records of both A and B, along with the C records joining them**
    Specify both A and B with ``extract: all: True``. Then list C afterwards, with ``extract: descendents: True``.
3. **We want to extract all C records, with their associated A and B records.**
    Specify C first, with ``extract: all: True``. Then list A and B in either order following C, and specify ``extract: descendents: True``. In this situation, Amaxa won't find any descendent records for A and B (since they are parents), but it will automatically pull all records associated to the extracted C records as dependencies.

When designing an operation, it's best to think in terms of which objects are primary for the operation, and take advantage of both descendent and dependent record tracing to build the operation sequence accordingly.

Specifying API Options
**********************

You can control how Amaxa uses the Salesforce API with the ``options`` key, which can be specified at the top level of the file as well as within each step of the operation. Step-level options take precedence over operation-level options.

The available options are:

- ``api-version``, the Salesforce API version to use (default: 52.0). This option may be specified only at the operation level.
- ``bulk-api-batch-size``, an integer between 0 and 10,000 (default: 10,000). This is the maximum record count of a batch uploaded by Amaxa. Reduce the batch size if your operations fail due to size errors from the Bulk API, such as ``Exceeded max size limit of 10000000`` (a limit on the total bytewise size of a batch). Note that the Bulk API batch size is not connected to the batch size used by Salesforce Data Loader when operated in REST API mode and does not impact the size of trigger invocations.
- ``bulk-api-timeout``, an integer greater than 0 (default: 1,200). The length of time, in seconds, to wait for a Bulk API batch to complete. Defaults to 1200 seconds (20 minutes).
- ``bulk-api-poll-interval``, an integer between 0 and 60 (default: 5). The length of time, in seconds, to wait between calls to check the Bulk API's status. Increase if you are running very large jobs and want to minimize API calls and log chatter.
- ``bulk-api-mode``, either ``Serial`` or ``Parallel`` (default: ``Parallel`). The Bulk API mode of operation. Serial mode may be selected to resolve some concurrency issues, such as ``UNABLE_TO_LOCK_ROW``.
