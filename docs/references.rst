Handling Outside References and Self-Lookups
--------------------------------------------

Amaxa provides options for controlling its reference-tracing behavior in two circumstances that can cause issues: self-lookups and outside references.

Self Lookups
************

A self-lookup is a relationship from an object to itself, such as ``Account.ParentId``. Amaxa's default behavior is to handle self-lookups by iterating both up and down the hierarchy to ensure that all parents and children linked to a specific extracted record are also extracted. For example, given the following Account hierarchy:

::

    Amalgamated Industries
      → Technology Refining Corp.
        → Global Research
        → Applied Neogenomics
      → Dyadic Operations Inc.
        → Rossum Ltd.

If we specify the ``Id`` of Dyadic Operations Inc. in an extract operation, Amaxa will recurse upwards to Amalgamated Industries, and back down through the hierarchy, ultimately extracting Dyadic Operations Inc. itself, its children, its parents and grandparents, and *their* children. Then, if a descendent sObject like ``Contact`` is also specified, the records associated will the entire Account hierarchy will be extracted.

If this behavior isn't desired, the ``self-lookup-behavior`` key can be applied at the sObject level or in the map for an individual field entry. The allowed values are ``trace-all``, the default, or ``trace-none``, which inhibits following self-lookups.

Self-lookup behavior can be configured both at the sObject level and within the mapping for an individual field.

Outside References
******************

An "outside reference", in Amaxa's terminology, is a reference from sObject B to sObject A, where

  - both sObjects are included in the operation;
  - sObject A is above sObject B;
  - the field value of the reference on some record of sObject B is the Id of a record of sObject A that was not extracted.

If Contact has a custom lookup to Account, ``Primary_Support_Account__c``, an outside reference could occur if Amaxa extracted Accounts A, B, and C and all of their Contacts, and then found that Contact D's ``Primary_Support_Account__c`` lookup referred to Account Q - a record that was not included in the extraction. Because Accounts were already extracted, Amaxa can't add that record as a dependency.

Amaxa offers special handling behaviors for outside references to help ensure that extracted data maintains referential integrity and can be loaded safely in another org.

Like with self-lookups, outside reference behavior is specified with a key, ``outside-lookup-behavior``, that can be placed at the sObject level or the field level in the definition file. The allowed options are

- ``include``, the default: include the outside reference in extracted data. (Errors may be thrown on load if the linked record is not present in the target environment).
- ``drop-field``: null out the outside reference when extracting and loading.
- ``error``: stop and record an error when an outside reference is found.

Note that references to sObjects that are mapped or aren't part of the operation at all are not considered outside references, and handler behavior is inactive for such references. For example, the ``OwnerId`` field is a reference to the ``Queue`` or ``User`` sObjects. If these sObjects are not included in the operation, specifying ``outside-lookup-behavior: drop-field`` will have no effect on the ``OwnerId`` field. Amaxa will log warnings when references to non-included sObjects are part of an operation.

Outside reference behavior can be very useful in situations with complex dependent reference networks. A Contact with a reference to an Account other than its own, as above, is likely to constitute an outside reference. Outside reference behaviors allow for omitting such lookups from the operation, ensuring that the data extracted does not contain dangling references.
