sfdx force:org:create -s -f assets/project-scratch-def.json -a amaxa
sfdx force:apex:execute -f assets/scripts/remove-junk-records.apex -u amaxa
source assets/scripts/get-auth-params.sh
