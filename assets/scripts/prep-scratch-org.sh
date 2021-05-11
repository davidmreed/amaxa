CURDIR=$(pwd)
sfdx force:org:create -s -f assets/scratch_orgs/config/project-scratch-def.json -a amaxa
sfdx force:apex:execute -f assets/scripts/remove-junk-records.apex -u amaxa
cd assets/scratch_orgs
sfdx force:source:push -u amaxa
cd $CURDIR
source assets/scripts/get-auth-params.sh
