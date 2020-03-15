# Amaxa Test Plan

Each released version of Amaxa is tested both via automated unit tests and is manually executed with an end-to-end data load and extract process using SFDX scratch orgs.

## End-to-End Testing Process

 1. Install the new version of Amaxa.

 1. Create a new scratch org and populate its credentials into environment variables using Amaxa's supplied script:

        sfdx force:org:create -s -f assets/project-scratch-def.json -a scratch
        source assets/scripts/get-auth-params.sh

 1. Run the Apex script to ensure Marketing User permission:

        sfdx force:apex:execute -f assets/scripts/UpdateUser.apex -u scratch

    The script ensures that Marketing User permission is applied to the scratch org user, because
    test processes require access to the Campaign object. An error in that specific component of
    the script is meaningless and does not constitute a test failure.

 1. Navigate to the test data folder:

        cd assets/test_data_csv

 1. Run the load:

        amaxa --load --credentials credentials-env.yml test.yml

    Validate that it completes without errors.

 1. Load the scratch org with `sfdx force:org:open -u scratch`.

 1. Validate the count loaded for each sObject using `count()` queries:

    - Account
    - Campaign
    - Contact
    - Lead
    - Opportunity
    - OpportunityContactRole

 1. Extract the data back from the org:

        amaxa --credentials credentials-env.yml test.yml

    Validate that it completes without errors.

 1. Validate the count extracted for each sObject:

    - Account
    - Campaign
    - Contact
    - Lead
    - Opportunity
    - OpportunityContactRole
