# About this Test Data Suite

This test data suite is a demonstration of using Amaxa to extract a hierarchy, in this case a multi-level Account hierarchy. A set of example data is included in Salesforce DX JSON format and can be loaded to a scratch org with SFDX. It's used as part of Amaxa's automated tests, and can also be used to experiment.

The `test.yml` file demonstrates using a query to identify specific records to extract, and shows how Amaxa recurses through the self-reference hierarchy. Note that the specified query pulls top-level Accounts only; Amaxa will automatically pull references. It also demonstrates selecting specific fields for each object and mapping fields to column names.

To try out extracting the sample data set, spin up a Salesforce DX scratch org and load the data with

    sfdx force:data:tree:import -p assets/test_data_sfdx/Account-Contact-plan.json -u scratch

Alternately, run

    source assets/scripts/prep-scratch-org.sh

which will handle creation and load for you.

Generate a password and security token for the scratch org user, and place these values in `credentials.yml`.

Then, change directories into `test_data_sfdx` and do

    amaxa --credentials credentials.yml test.yml

and compare the CSV data extracted with the JSON SFDX source.
