export BIN_PATH=$(curl --request POST --header "Private-Token: $API_TOKEN" --form "file=@dist/amaxa" "$CI_API_V4_URL/projects/$CI_PROJECT_ID/uploads" | jq -r .url)
export WHEEL_PATH=$(curl --request POST --header "Private-Token: $API_TOKEN" --form "file=@dist/amaxa-$CI_COMMIT_TAG-py3-none-any.whl" "$CI_API_V4_URL/projects/$CI_PROJECT_ID/uploads" | jq -r .url)
export SDIST_PATH=$(curl --request POST --header "Private-Token: $API_TOKEN" --form "file=@dist/amaxa-$CI_COMMIT_TAG.tar.gz" "$CI_API_V4_URL/projects/$CI_PROJECT_ID/uploads" | jq -r .url)
export DATA="{ \"name\": \"$CI_COMMIT_TAG\", \"tag_name\": \"$CI_COMMIT_TAG\", \"assets\": { \"links\": [{ \"name\": \"Binary\", \"url\": \"$CI_PROJECT_URL$BIN_PATH\" }, { \"name\": \"Wheel\", \"url\": \"$CI_PROJECT_URL$WHEEL_PATH\" }, { \"name\": \"Sdist\", \"url\": \"$CI_PROJECT_URL$SDIST_PATH\" }] } }"

curl --header "Content-Type: application/json" \
--header "Private-Token: $API_TOKEN" \
--data "$DATA" \
--request POST "$CI_API_V4_URL/projects/$CI_PROJECT_ID/releases"
