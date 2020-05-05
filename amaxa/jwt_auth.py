import datetime

import jwt
import requests
import simple_salesforce
from simple_salesforce.exceptions import SalesforceAuthenticationFailed


def jwt_login(consumer_id, username, private_key, api_version, sandbox=False):
    endpoint = (
        "https://test.salesforce.com"
        if sandbox is True
        else "https://login.salesforce.com"
    )
    jwt_payload = jwt.encode(
        {
            "exp": datetime.datetime.utcnow() + datetime.timedelta(seconds=30),
            "iss": consumer_id,
            "aud": endpoint,
            "sub": username,
        },
        private_key,
        algorithm="RS256",
    )

    result = requests.post(
        endpoint + "/services/oauth2/token",
        data={
            "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
            "assertion": jwt_payload,
        },
    )
    body = result.json()

    if result.status_code != 200:
        raise SalesforceAuthenticationFailed(body["error"], body["error_description"])

    return simple_salesforce.Salesforce(
        instance_url=body["instance_url"],
        session_id=body["access_token"],
        version=api_version,
    )
