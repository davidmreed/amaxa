# About this Test Data Suite

This test data suite is a demonstration of using Amaxa to extract or load a complex set of standard objects (Account, Contact, Opportunity, Lead, Campaign, and Opportunity Contact Role). A set of example data is included in CSV format and can be loaded to a Salesforce DX scratch org. It's used as part of Amaxa's automated tests, and can also be used to experiment.

The `test.yml` file is meant as an example of moving data between related orgs - such as a sandbox and Production, or two sandboxes. It uses field groups to define the data points to load and extract; because these field groups often include references to records like Users, it's most suitable for orgs that share those records. The example data set has been stripped of such non-independent references.

To try out loading the sample data set, spin up a Salesforce DX scratch org, sandbox, or developer org, and place the credentials in a `credentials.yml` file (an empty template is included). If using a Salesforce DX org, give the org the alias `amaxa`, and use the `credentials-sfdx.yml` file to automatically authenticate into it.

Then, change into the `test_data_csv` directory and do

    amaxa --load --credentials credentials.yml test.yml

and view the results in your scratch org.

You can also do

    amaxa --credentials credentials.yml test.yml

to replace the test data with data pulled from your target org.
