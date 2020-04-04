# About this Test Data Suite

This test data suite is a demonstration of using Amaxa to apply both custom and built-in transformations on data during load and extract operations. A set of example data is included in CSV format and can be loaded to a Salesforce DX scratch org. It's used as part of Amaxa's automated tests, and can also be used to experiment.

The `test.yml` file defines an operation that loads only one sObject (`Account`). It applies two transformations: the `Name` field is lowercased, and a custom transformation (defined in `test_transforms.py`) called `multiply` is applied to the `Description` field. `multiply` takes options in the operation definition, here an integer `count` for how many times to repeat the content of the field.

`test_transforms.py` is an example of how to implement your own transform logic for data operations. Note that the operation definition loads this file via the `plugin-modules` key.

To try out loading the sample data set, spin up a Salesforce DX scratch org, sandbox, or developer org, and place the credentials in a `credentials.yml` file (an empty template is included). If using a Salesforce DX org, give the org the alias `amaxa`, and use the `credentials-sfdx.yml` file to automatically authenticate into it.

Then, change into the `test_data_transforms` directory and do

    amaxa --load --credentials credentials.yml test.yml

and view the results in your scratch org.

You can also do

    amaxa --credentials credentials.yml test.yml

to replace the test data with data pulled from your target org.
