import argparse
import yaml
import json
from . import amaxa, loader

def main():
    a = argparse.ArgumentParser()

    a.add_argument('config', type=argparse.FileType('r'))
    a.add_argument('-c', '--credentials', required=True, dest='credentials', type=argparse.FileType('r'))

    args = a.parse_args()

    credentials = None

    # Grab the credential file first. We need it to validate the extraction.
    f = args.credentials
    if f.name.endswith('json'):
        credentials = json.load(f)
    else:
        credentials = yaml.safe_load(f)

    (context, errors) = loader.load_credentials(credentials)

    if context is None:
        print('The supplied credentials were not valid: {}'.format('\n'.join(errors)))
        return -1

    if args.config.name.endswith('json'):
        config = json.load(args.config)
    else:
        config = yaml.safe_load(args.config)

    (ex, errors) = loader.load_extraction(config, context)

    if ex is not None:
        ex.execute()
    else:
        print('Unable to execute operation due to the following errors: {}'.format('\n'.join(errors)))
        return -1

    return 0