Data Transforms
---------------

Introduction
************

Amaxa supports applying *transforms* to data values during both extraction and load operations. Transforms are specified at the field level in the operation definition file.

.. code-block:: yaml

    version: 2
    plugin-modules:
        - test_transforms
    operation:
        - sobject: Account
        fields:
            - field: Name
              transforms:
                - uppercase
                - name: suffix
                  options:
                    suffix: "-CLIENT"
        extract:
            all: True

In this example, the `Name` field on `Account` has two transforms applied to it: ``uppercase`` and ``suffix``. Transforms are applied sequentially, and can take options as shown above. Loading an Account with the Name "Acme Products" (in its CSV file) via the operation definition shown above would yield an Account named "ACME PRODUCTS-CLIENT" in the target Salesforce org.

Transforms are bidirectional: they run on both load and extract.

Amaxa ships with five transforms:

- ``uppercase`` uppercases the field value.
- ``lowercase`` lowercases the string value.
- ``strip`` removes leading and trailing whitespace.
- ``prefix`` adds a prefix string (specified with the ``options`` key ``prefix``).
- ``suffix`` adds a suffix string (specified with the ``options`` key ``suffix``).

Custom Transforms
*****************

Amaxa also supports custom transforms. To implement a custom transformation, create a Python module containing a class that subclasses ``amaxa.transforms.TransformProvider``. Subclasses must populate the ``transform_name`` class attribute and override two methods, ``get_transform()`` and ``get_options_schema()``.

``get_transform()`` accepts the API name of the configured field and a ``dict`` containing the user-specified options, if any. It returns a callable that will be invoked with a single parameter, the field value, each time the transform is used.

``get_options_schema()`` returns the schema by which the user-specifiable options will be validated in the operation definition. This is a `Cerberus <https://docs.python-cerberus.org/>`_ schema that should validate a ``dict``.

Before being used in an operation, custom transforms must be loaded by specifying them in a ``plugin-modules`` list at the top level of the YAML configuration. Specify the name of the containing module, not the transform class; Amaxa will automatically discover transforms in those modules. Modules may be located in the current working directory or anywhere in the Python search path.

A complete example of using a custom transform is included in Amaxa's repository in the ``assets/test_data_transforms`` directory.
