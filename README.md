# Amaxa - a multi-object ETL tool for Salesforce

Amaxa is a new data loader and ETL (extract-transform-load) tool for Salesforce, designed to support the extraction and loading of complex networks of records in a single operation. For example, an Amaxa operation can extract a designated set of Accounts, their associated Contacts and Opportunities, their Opportunity Contact Roles, and associated Campaigns, and then load all of that data into another Salesforce org while preserving the connections between the records.

Amaxa is designed to replace complex, error-prone workflows that manipulate data exports with `VLOOKUP()` to maintain object relationships.

## Installing, Building, and Testing Amaxa

Using Amaxa requires Python 3.6. To install Amaxa using `pip`, execute

    $ pip install amaxa

Make sure to invoke within a Python 3.6+ virtual environment or specify Python 3.6 or greater as required by your operating system.

Amaxa is operating system-agnostic. It has been tested primarily on Linux but is also known to work in a MINGW Windows 7 environment.

Using Amaxa within Docker, execute

    $ docker build -t amaxa:my_local_amaxa .
   
To start the container such that you could run Amaxa commands, execute

    $ docker run -it amaxa:my_local_amaxa bash

### Development

To start working with Amaxa in a virtual environment, clone the Git repository. Then, create a virtual environment for Amaxa and install there:

    $ cd amaxa
    $ python3.6 -m venv venv
    $ source venv/bin/activate
    $ pip install -r requirements.txt -r testing-requirements.txt
    $ python setup.py install

You'll then be able to invoke `amaxa` from the command line whenever the virtual environment is active.

Amaxa depends on the following packages to run:

 - `simple_salesforce`
 - `salesforce-bulk`
 - `pyyaml`
 - `pyjwt`
 - `cryptography`
 - `requests`
 - `cerberus`

 For development and testing, you'll also need

 - `pytest`
 - `pytest-cov`
 - `codecov`
 - `wheel`
 - `setuptools`

Tests are executed using `pytest`. If a valid Salesforce access token and instance URL are present in the environment variables `INSTANCE_URL` and `ACCESS_TOKEN`, integration and end-to-end tests will be run against that Salesforce org; otherwise only unit tests are run. Note that **integration tests are destructive** and require data setup before running. Run integration tests **only** in a Salesforce DX scratch org (see `.gitlab-ci.yml` for the specific testing process).

## Running Amaxa

The command-line API is very simple. To extract data, given an operation definition file `op.yml` and a credential file `cred.yml`, run

    $ amaxa --credentials cred.yml op.yml

To perform the load of the same operation definition, just add `--load`:

    $ amaxa --credentials cred.yml op.yml --load

Operation definitions are generally built to support both load and extract of the same object network. For details, see below. While the examples in this guide are in YAML format, Amaxa supports JSON at feature parity and with the same schemas.

The only other command-line switch provided by Amaxa is `--verbosity`. Supported levels are `quiet`, `errors`, `normal`, and `verbose`, in ascending order of verbosity.

To see usage help, execute

    $ amaxa --help

## Supplying Credentials

Credentials are supplied in a YAML or JSON file, as shown here (for username and password)

    version: 1
    credentials:
        username: 'test@example.com'
        password: 'blah'
        security-token: '00000'
        sandbox: True

Amaxa also allows JWT authentication, for headless operation:

    version: 1
    credentials:
        username: 'test@example.com'
        consumer-key: 'GOES_HERE'
        jwt-key: |
        -----BEGIN RSA PRIVATE KEY-----
        <snipped>
        -----END RSA PRIVATE KEY-----
        sandbox: False

If your JWT key is stored externally in a file, use the key `jwt-file` with the name of that file rather than including the key inline.

Lastly, if you establish authentication outside Amaxa (with Salesforce DX, for example), you can directly provide an access token and instance URL.

    version: 1
    credentials:
        access-token: '.....'
        instance-url: 'https://test.salesforce.com

## Defining Operations

Operations run with Amaxa are established by an operation definition file written in either JSON or YAML. The operation definition specifies which sObjects to extract or load in which order, and which fields on each object are desired. Amaxa handles tracing relationships between top-level objects and their children and extracts a set of CSV files to produce a complete, internally consistent data set.

Here's an example of an Amaxa operation definition in YAML.

    version: 1
    operation:
        -
            sobject: Account
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

The meat of the definition is list of sObjects under the `operation` key. We list objects in order of their extraction or load. The order is important, because it plays into how Amaxa will locate, extract, and load relationships between the objects. Typically, you should start an operation definition with the highest-level object you want to extract or load. This may be `Account`, for example, or if loading a custom object with children, the parent object. Then, we list child objects and other dependencies in order below it. Objects listed later in the list may be extracted based on lookup or master-detail relationships to objects higher in the list. (More details on object-sequence patterns can be found below.)

For each object we choose to extract, we answer a few main questions.

### Which records do we want to extract?

Record selection is specified with the `extract:` key. We can specify several different types of record-level extraction mechanics. The `extract` key is ignored during load operations.

    query: 'Industry = "Non-Profit"'

The `query` type of extraction pulls records that match a SOQL `WHERE` clause that you supply. (Do not include the `WHERE` keyword).

    descendents: True

The `descendents` type of extraction pulls records that have a lookup or master-detail relationship to any object higher in the operation definition. This relationship can be any field that is included in the selected fields for the object. For example, if extracting `Account` followed by `Contact`, with `descendents: True` specified, Amaxa will pull Contacts associated to all extracted Accounts via *any lookup field from `Contact` to `Account` that is included in the operation*. This could, for example, include `AccountId` as well as some custom field `Other_Account__c`. If another object were above `Contact` in the operation and `Contact` has a relationship to that object, Amaxa would also pull `Contact` records associated to extracted records for that object.

    ids:
        - 003000000000001
        - 003000000000002
        - 003000000000003

The `ids` type of extraction pulls specific records by `Id`, supplied in a list.

All types of extraction also retrieve *dependent relationships*. When an sObject higher in the operation has a relationship to an sObject lower in the operation, the Ids of referenced objects are recorded and extracted later in the process. For example, if an included field on `Account` is a relationship `Primary_Contact__c` to `Contact`, but `Account` is extracted first, Amaxa will ensure that all referenced records are extracted during the `Contact` step.

The combination of dependent and descendent relationship tracing helps ensure that Amaxa extracts and loads an internally consistent slice of your org's data based upon the operation definition you provide.

### Which fields do we want to extract or load?

This is specified with the `fields` or `field-group` keys.

The easiest way to select fields is to specify `field-group: [smart|readable|writeable]`. This instructs Amaxa to automatically determine which fields to extract based on access level: `readable` is all accessible fields, `writeable` all createable and updateable fields, and `smart` will automatically select `readable` for extract operations and `writeable` for loads. The use of field groups streamlines the configuration file, but is most suitable for extract and load operations performed on the same org or related orgs, like sandboxes derived from the same production org. This is because Amaxa will extract references to, for example, Record Types and Users whose Ids may differ across unrelated orgs.

If you're moving data between unrelated orgs or wish to specify the exact field set for each sObject, use the `fields` key. The value of this key is a list whose elements are either the API name of a single field or a map specifying how to load, extract, and transform the data.

    fields:
        - Name
        - Industry

is an example of a simple field specification.

    fields:
        -
            field: Description
            column: Desc
            transforms:
                - strip
                - lowercase

would extract the `Description` field, name the CSV column `Desc`, and apply the transformations `strip` (remove leading and trailing whitespace) and `lowercase` (convert text to lower case) on extracted data. On load, Amaxa would look for a CSV column `Desc`, map it to the `Description` field, and apply the same transformations to inbound data.

### Where is the data going to or coming from?

The `file` key for each sObject specifies a CSV file. This is the input data for a load operation, or the output data for an extraction. Amaxa will specify `sObjectName.csv` if the key is not provided.

For loads, Amaxa will also use a `result-file` key, which specifies the location for the output Id map and error file. If not supplied, Amaxa will use `sObjectName-results.csv`. The results file has three columns: `"Original Id"`, `"New Id"`, and `"Error"`.

### Object sequencing in an operation

As shown in the example above, to extract or load a parent object and its children, list the parent first, followed by the child, and specify `extract: descendents: True` for the child. If the parent is itself a child of a higher-level parent, you can use `descendents` there too - just make sure your operation definition starts with at least one object that is configured with `extract: all: True`, `extract: ids: <list>`, or `extract: query: <where clause>` so that Amaxa has a designated record set with which to begin.

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

In this pattern, Amaxa will not find any descendents for `Account` (unless `Account` itself has a lookup to `Contact`), but it will pull the parent Accounts of all of the extracted Contacts as dependencies, because the `AccountId` field is included in the operation.

Junction objects may be selected in several different ways. Suppose we have objects A and B, joined with a junction object C.

 1. **We want to extract specific A records, with all of their C junctions and the B records associated to them.**
    We specify A first, then C, then B. C and B have `descendents: True` set under `extract:`.
 2. **We want to extract all records of both A and B, along with the C records joining them**
    Specify both A and B with `extract: all: True`. Then list C afterwards, with `extract: descendents: True`.
 3. **We want to extract all C records, with their associated A and B records.**
    Specify C first, with `extract: all: True`. Then list A and B in either order following C, and specify `extract: descendents: True`. In this situation, Amaxa won't find any descendent records for A and B (since they are parents), but it will automatically pull all records associated to the extracted C records as dependencies.

When designing an operation, it's best to think in terms of which objects are primary for the operation, and take advantage of both descendent and dependent record tracing to build the operation sequence accordingly.

## Validation

Amaxa tries to warn you if you specify an operation that doesn't make sense or is invalid.

Both sObjects and `fields` entries are checked before the operation begins. All entries will be validated to ensure they exist and are accessible to the running user. Amaxa will show an error and stop if fields cannot be written or updated for a load operation (only dependent lookups must be updateable, but all fields must be createable).

Amaxa will also validate, for load operations, that the specified input data matches the operation definition. For field lists specified with `fields`, the column set in the provided CSV must exactly match the field list (taking any specified `column` mappings into account). For `field-group` specifications, Amaxa allows fields that are part of the field group to be omitted from the CSV, but does not allow any extra fields in the CSV. If the `field-group: smart` choice is provided, Amaxa always validates against the `readable` field group, even on load, but will only attempt to load writeable fields.

You can control validation at the sObject level by specifying the key `input-validation` within each entry. The acceptable values are 

  - `none`, which completely disables validation and is not recommended.
  - `default`, which applies the default semantics above.
  - `strict`, which treats `field-group` entries just like `fields`, meaning that there must be a 1:1 match between file columns and fields.

## Handling Outside References and Self-Lookups

Amaxa provides options for controlling its reference-tracing behavior in two circumstances that can cause issues: self-lookups and outside references.

### Self Lookups

A self-lookup is a relationship from an object to itself, such as `Account.ParentId`. Amaxa's default behavior is to handle self-lookups by iterating both up and down the hierarchy to ensure that all parents and children linked to a specific extracted record are also extracted. For example, given the following Account hierarchy:

    Amalgamated Industries
      → Technology Refining Corp.
        → Global Research
        → Applied Neogenomics
      → Dyadic Operations Inc.
        → Rossum Ltd.

If we specify the `Id` of Dyadic Operations Inc. in an extract operation, Amaxa will recurse upwards to Amalgamated Industries, and back down through the hierarchy, ultimately extracting Dyadic Operations Inc. itself, its children, its parents and grandparents, and *their* children. Then, if a descendent sObject like `Contact` is also specified, the records associated will the entire Account hierarchy will be extracted.

If this behavior isn't desired, the `self-lookup-behavior` key can be applied at the sObject level or in the map for an individual field entry. The allowed values are `trace-all`, the default, or `trace-none`, which inhibits following self-lookups.

Self-lookup behavior can be configured both at the sObject level and within the mapping for an individual field.

### Outside References

An "outside reference", in Amaxa's terminology, is a reference from sObject B to sObject A, where

  - both sObjects are included in the operation;
  - sObject A is above sObject B;
  - the field value of the reference on some record of sObject B is the Id of a record of sObject A that was not extracted.

If Contact has a custom lookup to Account, `Primary_Support_Account__c`, an outside reference could occur if Amaxa extracted Accounts A, B, and C and all of their Contacts, and then found that Contact D's `Primary_Support_Account__c` lookup referred to Account Q - a record that was not included in the extraction. Because Accounts were already extracted, Amaxa can't add that record as a dependency.

Amaxa offers special handling behaviors for outside references to help ensure that extracted data maintains referential integrity and can be loaded safely in another org.

Like with self-lookups, outside reference behavior is specified with a key, `outside-lookup-behavior`, that can be placed at the sObject level or the field level in the definition file. The allowed options are

 - `include`, the default: include the outside reference in extracted data. (Errors may be thrown on load if the linked record is not present in the target environment).
 - `drop-field`: null out the outside reference when extracting and loading.
 - `error`: stop and record an error when an outside reference is found.

Note that references to sObjects that aren't part of the operation at all are not considered outside references, and handler behavior is inactive for such references. For example, the `OwnerId` field is a reference to the `Queue` or `User` sObjects. If these sObjects are not included in the operation, specifying `outside-lookup-behavior: drop-field` will have no effect on the `OwnerId` field. Amaxa will log warnings when references to non-included sObjects are part of an operation.

Outside reference behavior can be very useful in situations with complex dependent reference networks. A Contact with a reference to an Account other than its own, as above, is likely to constitute an outside reference. Outside reference behaviors allow for omitting such lookups from the operation, ensuring that the data extracted does not contain dangling references.

## Error Behavior and Recovery

Because error recovery when loading complex object networks can be challenging and the overall load operation is not atomic, it's strongly recommended that all triggers, workflow rules, processes, validation rules, and lookup field filters be deactivated during an Amaxa load process. It's far easier to prevent errors than to fix them.

Amaxa executes loads in two stages, called *inserts* and *dependents*. In the *inserts* phase, Amaxa loads records of each sObject in sequence. In the *dependents* phase, Amaxa runs updates to populate self-lookups and dependent lookups on the created records. In both phases, Amaxa stops loading data when it receives an error from Salesforce. Since Amaxa uses the Bulk API, the stoppage occurs at the end of the sObject and phase that's currently processing. 

If Accounts, Contacts, and Opportunities are being loaded, and an error occurs during the insert of Contacts, Amaxa will stop at the end of the Contact insert phase. All successfully loaded Accounts and Contacts remain in Salesforce, but no work is done for the *dependents* phase. If the error occurs during the *dependents* phase, all records of all sObjects have been loaded, but dependent and self-lookups for the errored sObject and all sObjects later in the operation are not populated. 

Details of the errors encountered are shown in the results file for the errored sObject, which by default is `sObjectName-results.csv` but can be overridden in the operation definition.

When Amaxa stops due to errors, it saves a *state file*, which preserves the phase and progress of the load operation. The state file for some operation `operation.yaml` will be called `operation.state.yaml`. The state file persists the map of old to new Salesforce Ids that were successfully loaded, as well as the position the operation was in when the failure occured.

Should a failure occur, you can take action to remediate the failure, including making changes to the records in your `.csv` files or altering the metadata in your org. You can then resume the load by repeating your original command and adding the `-s <state-file>` option:

    $ amaxa --load operation.yaml -c credentials.yaml -s operation.state.yaml

Amaxa will pick up where it left off, loading only the records which failed or which weren't loaded the first time. (You may add records to the operation, in any sObject, and Amaxa will pick them up upon resume provided that the original failure was in the *inserts* phase - do not add new records if Amaxa has reached the *dependents* phase). It will also complete any un-executed passes to populate dependent and self-lookups.

## API Usage

Amaxa uses both the REST and Bulk APIs to do its work.

When extracting, it consumes one Bulk API job for each sObject with `extract` set to `all` or `query`, plus approximately one API call (to the REST API) per 200 records that are extracted by Id due to dependencies or `extract` set to `descendents`.

When loading, Amaxa uses one Bulk API batch for each 10,000 records of each sObject, plus one Bulk API batch for each 10,000 records of each sObject that has self- or dependent lookups. Only records requiring dependent processing are included in the second phase.

A small number of additional API calls are used on each operation to obtain schema information for the org.

## Example Data and Test Suites

Two example data suites and operation definition files are included with Amaxa in the `assets` directory. See `about.md` in each directory for information about what the data suite includes and tests and how to use it.

## Limitations, Known Issues, and Future Plans

 - Amaxa does not support import or export of compound fields (Addresses and Geolocations), but can import and export their component fields, such as `MailingStreet`.
 - Amaxa does not support Base64 binary-blob fields.

Future plans include:

 - Improvements to efficiency in API use and memory consumption.
 - More sophisticated handling of references to "metadata-ish" sObjects, like Users and Record Types.
 - Support for importing data from external systems that does not have a Salesforce Id
   - Note that manually synthesizing Id values in input data is fine, provided they conform to the expected length and character content of Salesforce Ids.
 - Recursive logic on extraction to handle outside references.

## What Does Amaxa Mean?

[ἄμαξα](http://www.perseus.tufts.edu/hopper/text?doc=Perseus%3Atext%3A1999.04.0058%3Aentry%3Da\)%2Fmaca) is the Ancient Greek word for a wagon.
