import simple_salesforce
import argparse
import yaml
import json
from . import schema, amaxa, loader

def main():
    a = argparse.ArgumentParser()

    a.add_argument('config', dest='config', type=argparse.FileType('r'))
    a.add_argument('-c', '--credentials', dest='credentials', type=argparse.FileType('r'))

    args = a.parse_args()

    credentials = None

    # If we have a credential file specified separately, grab that first.
    if hasattr(args, 'credentials'):
        f = args.credentials
        if f.name.endswith('json'):
            credentials = json.load(f)
        else:
            credentials = yaml.safe_load(f)

    if args.config.name.endswith('json'):
        config = json.load(args.config)
    else:
        config = yaml.safe_load(args.config)

    loader.load_extraction(config, credentials).execute()
