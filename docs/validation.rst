Validation
----------

Amaxa tries to warn you if you specify an operation that doesn't make sense or is invalid.

Both sObjects and ``fields`` entries are checked before the operation begins. All entries will be validated to ensure they exist and are accessible to the running user. Amaxa will show an error and stop if fields cannot be written or updated for a load operation (only dependent lookups must be updateable, but all fields must be createable).

Amaxa will also validate, for load operations, that the specified input data matches the operation definition. For field lists specified with ``fields``, the column set in the provided CSV must exactly match the field list (taking any specified ``column`` mappings into account). For ``field-group`` specifications, Amaxa allows fields that are part of the field group to be omitted from the CSV, but does not allow any extra fields in the CSV. If the ``field-group: smart`` choice is provided, Amaxa always validates against the ``readable`` field group, even on load, but will only attempt to load writeable fields.

You can control validation at the sObject level by specifying the key ``input-validation`` within each entry. The acceptable values are

  - ``none``, which completely disables validation and is not recommended.
  - ``default``, which applies the default semantics above.
  - ``strict``, which treats ``field-group`` entries just like ``fields``, meaning that there must be a 1:1 match between file columns and fields.
