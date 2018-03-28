import argparse
import yaml

def main():
    a = argparse.ArgumentParser()

    a.add_argument('-u', '--username', dest='user')
    a.add_argument('-p', '--password', dest='password')
    a.add_argument('-t', '--security-token', dest='token')
    a.add_argument('-a', '--access-token', dest='access_token')
    a.add_argument('-i', '--instance-url', dest='instance_url')
    a.add_argument('-s', '--sandbox', dest='sandbox', action="store_true")

    args = a.parse_args()

    connection = None

    if args.access_token is not None and args.instance_url is not None:
        connection = simple_salesforce.Salesforce(instance_url=args.instance_url, 
                                                  session_id=args.access_token)
    elif args.user is not None and args.password is not None and args.token is not None:
        connection = simple_salesforce.Salesforce(username=args.user, 
                                                  password=args.password, 
                                                  security_token=args.token, 
                                                  sandbox=args.sandbox)
                                                  