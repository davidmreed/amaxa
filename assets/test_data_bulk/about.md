# About this Test Data Suite

This test data suite is intended to exercise bulk data loading. It is a simple file containing 100,000 Accounts.

To try out loading the sample data set, spin up a Salesforce DX scratch org, sandbox, or developer org, and place the credentials in a `credentials.yml` file (an empty template is included). If using a Salesforce DX org, give the org the alias `amaxa`, and use the `credentials-sfdx.yml` file to automatically authenticate into it.

Then, change into the `test_data_bulk` directory and do

    amaxa --load --credentials credentials.yml test.yml

and view the results in your scratch org.

You can also do

    amaxa --credentials credentials.yml test.yml

to replace the test data with data pulled from your target org.
