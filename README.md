# Amaxa - a multi-object ETL tool for Salesforce

Amaxa is a new data loader and ETL (extract-transform-load) tool for Salesforce, designed to support the extraction and loading of complex networks of records in a single operation. For example, an Amaxa operation can extract a designated set of Accounts, their associated Contacts and Opportunities, their OpportunityContactRoles, and associated Campaigns, and then load all of that data into another Salesforce org while preserving the connections between the records.

Amaxa is designed to replace complex, error-prone workflows that manipulate data exports with `VLOOKUP()` to maintain object relationships.

## Installing, Building, and Testing Amaxa

Amaxa requires Python 3.6 and the packages `simple_salesforce`, `salesforce-bulk`, `pyyaml`, and `cerberus`. Additional packages are required for development and testing (see `requirements.txt` and `testing-requirements.txt`). Amaxa is operating system-agnostic, but has been tested only on Linux.

To start working with Amaxa in a virtual environment, clone the Git repository. Then, create a virtual environment for Amaxa and install there:

    $ cd amaxa
    $ python3.6 -m venv venv
    $ source venv/bin/activate
    $ pip install -r requirements.txt -r testing-requirements.txt
    $ python setup.py install

You'll then be able to invoke `amaxa` from the command line whenever the virtual environment is active.

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

Credentials are supplied in a YAML or JSON file, as shown here.

    version: 1
    credentials:
        username: 'test@example.com'
        password: 'blah'
        security-token: '00000'
        sandbox: True
        
Amaxa doesn't currently support JWT authentication, but this is a planned future option.

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

The meat of the definition is list of sObjects under the `operation` key. We list objects in order of their extraction or load. The order is important, because it plays into how Amaxa will locate, extract, and load relationships between the objects. Generally speaking, you should start an operation definition with the highest-level object you want to extract or load. This may be `Account`, for example, or if loading a custom object with children, the parent object. Then, we list child objects and other dependencies in order below it. Objects listed later in the list may be extracted based on lookup or master-detail relationships to objects higher in the list.

For each object, we answer a few main questions.

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

## Validation

Amaxa tries to warn you if you specify an operation that doesn't make sense or is invalid. 

Both sObjects and `fields` entries are checked before the operation begins. All entries will be checked to ensure they exist and are accessible to the running user. Amaxa will show an error and stop if fields cannot be written or updated for a load operation (only dependent lookups must be updateable, but all fields must be createable). 

Amaxa will also validate, for load operations, that the specified input data matches the operation definition. For field lists specified with `fields`, the column set in the provided CSV must exactly match the field list (taking any specified `column` entries into account). For `field-group` specifications, Amaxa allows fields that are part of the field group to be omitted from the CSV, but does not allow any extra fields in the CSV. If the `field-group: smart` choice is provided, Amaxa always validates against the `readable` field group, even on load, but will only attempt to load writeable fields.

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

If this behavior isn't desired, the `self-lookup-behavior` key can be applied at the sObject level or in the map for an individual field entry. The allowed values are `trace-all`, the default, or `trace-none`, which inhibits following self-lookups for that object or for that specific self-lookup field.

### Outside References

An "outside reference", in Amaxa's terminology, is a reference from sObject B to sObject A, where 

  - both sObjects are included in the operation;
  - sObject A is above sObject B;
  - the field value of the reference on some record of sObject B is the Id of a record of sObject A that was not extracted.

Amaxa offers special handling behaviors for outside references to help ensure that extracted data maintains referential integrity and can be loaded safely in another org.

Like with self-lookups, outside reference behavior is specified with a key, `outside-lookup-behavior`, that can be placed at the sObject level or the field level in the definition file. The allowed options are

 - `include`, the default: include the outside reference in extracted data. (Errors may be thrown on load if the linked record is not present in the target environment).
 - `drop-field`: null out the outside reference when extracting and loading.
 - `error`: stop and record an error when an outside reference is found.

Note that references to sObjects that aren't part of the operation at all are not considered outside references, and handler behavior is inactive for such references. For example, the `OwnerId` field is a reference to the `Queue` or `User` sObjects. If these sObjects are not included in the operation, specifying `outside-lookup-behavior: drop-field` will have no effect on the `OwnerId` field. 

Outside reference behavior can be very useful in situations with complex dependent reference networks. A Contact with a reference to an Account other than its own, for example, is likely to constitute an outside reference. Outside reference behaviors allow for omitting such lookups from the operation, ensuring that the data extracted does not contain dangling references.

## Error Behavior

Amaxa stops loading data when it receives an error from Salesforce. Because Amaxa uses the Bulk API, in many cases this will take place after a majority of the records for a given sObject have been loaded.

When an error is encountered, operations stop immediately. This means that the last sObject to be loaded may not have self and dependent lookups populated, and some or most of the last sObject loaded may have been created in the target environment. Details of the errors encountered are shown in the results file, which by default is `sObjectName-results.csv`.

Recovering from errors can be challenging. At present, the best solution is to remove all loaded records and restart the operation. Future versions of Amaxa will provide sophisticated recovery or stop-and-resume support.

Because error recovery when loading complex object networks is difficult and the overall load operation is not atomic, it's strongly recommended that all triggers, workflow rules, processes, validation rules, and lookup field filters be deactivated during the load process.

## API Usage

Amaxa uses both the REST and Bulk APIs to do its work. 

When extracting, it consumes one Bulk API job for any sObject with `extract` set to `all` or `query`, plus approximately one API call (to the REST API) per 200 records that are extracted by Id due to dependencies or `extract` set to `descendents`. 

When loading, Amaxa uses one Bulk API job for each sObject, plus one Bulk API job for each sObject that has self or dependent lookups.

A small number of additional API calls are used on each operation to obtain schema information for the org.

## Example Data and Test Suites

Two example data suites and operation definition files are included with Amaxa in the `assets` directory. See `about.md` in each directory for information about what the data suite includes and tests and how to use it.

## Limitations, Known Issues, and Future Plans

 - Amaxa does not support import or export of compound fields (Addresses and Geolocations), but can import and export their component fields, such as `MailingStreet`.
 - Amaxa does not support Base64 binary-blob fields.
 - Tests are not effectively abstracted and are fairly repetitive. 

Future plans include:

 - Improvements to efficiency in API use and memory consumption.
 - More sophisticated handling of references to "metadata-ish" sObjects, like Users and Record Types.
 - Error recovery and pause/continue workflows.
 - Support for importing data from external systems that does not have a Salesforce Id
   - Note that synthesizing the Id in input data is perfectly fine.
 - Recursive logic on extraction to handle outside references.
 

## What Does Amaxa Mean?

[ἄμαξα](http://www.perseus.tufts.edu/hopper/text?doc=Perseus%3Atext%3A1999.04.0058%3Aentry%3Da\)%2Fmaca) is the Ancient Greek word for a wagon.