# Amaxa - a multi-object ETL tool for Salesforce

Amaxa is a new data loader and ETL (extract-transform-load) tool for Salesforce, designed to support the extraction and loading of complex networks of records in a single operation. For example, an Amaxa operation can extract a designated set of Accounts, their associated Contacts and Opportunities, their OpportunityContactRoles, and associated Campaigns, and then load all of that data into another Salesforce org while preserving the connections between the records.

Amaxa is designed to replace complex, error-prone workflows that manipulate data exports with `VLOOKUP()` to maintain object relationships.

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

For each object, we answer two main questions:

**Which records do we want to extract?** This is specified with the `extract:` key. We can specify several different types of record-level extraction mechanics.

    query: 'Industry = "Non-Profit"'
    
The `query` type of extraction pulls records that match a SOQL `WHERE` clause that you supply.

    descendents: True

The `descendents` type of extraction pulls records that have a lookup or master-detail relationship to any object higher in the operation definition. This relationship can be any field that is included in the selected fields for the object. For example, if extracting `Account` followed by `Contact`, with `descendents: True` specified, Amaxa will pull Contacts associated to all extracted Accounts via *any lookup field from `Contact` to `Account` that is included in the operation*. This could, for example, include `AccountId` as well as some custom field `Other_Account__c`. If another object were above `Contact` in the operation and `Contact` has a relationship to that object, Amaxa would also pull `Contact` records associated to extracted records for that object.

    ids:
        - 003000000000001
        - 003000000000002
        - 003000000000003

The `ids` type of extraction pulls specific records by `Id`, supplied in a list.

All types of extraction also retrieve *dependent relationships*. When an sObject higher in the operation has a relationship to an sObject lower in the operation, the Ids of referenced objects are recorded and extracted later in the process. For example, if an included field on `Account` is a relationship `Primary_Contact__c` to `Contact`, but `Account` is extracted first, Amaxa will ensure that all referenced records are extracted during the `Contact` step.

**Which fields do we want to extract?** This is specified with the `fields` or `field-group` keys.

The easiest way to select fields is to specify `field-group: [smart|readable|writeable]`. This instructs Amaxa to automatically determine which fields to extract based on access level: `readable` is all accessible fields, `writeable` all createable and updateable fields, and `smart` will automatically select `readable` for extract operations and `writeable` for loads. The use of field groups streamlines the configuration file, but is most suitable for extract and load operations performed on the same org or related orgs, like sandboxes derived from the same production org. This is because Amaxa will extract references to, for example, Record Types and Users that may differ across unrelated orgs.

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
                
would extract the `Description` field, name the CSV column `Desc`, and apply the transformations `strip` (remove leading and trailing whitespace) and `lowercase` (convert text to lower case) on both extracted and loaded data.
    