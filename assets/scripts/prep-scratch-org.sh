sfdx force:org:create -v DevHub -s -f assets/project-scratch-def.json -a scratch
sfdx force:data:tree:import -p assets/test_data_sfdx/Account-Contact-plan.json -u scratch
source assets/scripts/get-auth-params.sh