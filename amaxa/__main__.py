import argparse
import yaml

def main():
    a = argparse.ArgumentParser()

    a.add_argument('config', dest='config', type=argparse.FileType('r'))
    a.add_argument('-c', '--credentials', dest='credentials', type=argparse.FileType('r'))

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
                                                  