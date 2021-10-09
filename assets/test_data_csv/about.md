# About this Test Data Suite

This test data suite is a demonstration of using Amaxa to extract or load a complex set of standard objects (Account, Contact, Opportunity, Lead, Campaign, and Opportunity Contact Role). A set of example data is included in CSV format and can be loaded to a Salesforce DX scratch org. It's used as part of Amaxa's automated tests, and can also be used to experiment. The `test.yml` file in this data set demonstrates several Amaxa features, including field groups, junction objects, and object mapping.

To try out loading the sample data set, spin up a scratch org and deploy the metadata in `assets/scratch_orgs` into it. You can use the script `assets/scripts/prep-scratch-org.sh` to so. Make sure to give the scratch org the alias `amaxa`. Then, change into the `test_data_csv` directory and do

    amaxa --load --credentials credentials-sfdx.yml test.yml

and view the results in your scratch org.

You can also do

    amaxa --credentials credentials-sfdx.yml test.yml

to replace the test data with data pulled from your target org.
