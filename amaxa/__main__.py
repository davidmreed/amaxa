import argparse
import logging
import yaml
import json
from . import amaxa, loader

def main():
    a = argparse.ArgumentParser()

    a.add_argument('config', type=argparse.FileType('r'))
    a.add_argument('-c', '--credentials', required=True, dest='credentials', type=argparse.FileType('r'))
    verbosity_levels = {'quiet': logging.NOTSET, 'errors': logging.ERROR,
                        'normal': logging.INFO, 'verbose': logging.DEBUG}

    a.add_argument('-v', '--verbosity',
                   choices=verbosity_levels.keys(),
                   dest='verbosity', default='normal',
                   help='Log all actions')

    args = a.parse_args()

    logging.getLogger('amaxa').setLevel(verbosity_levels[args.verbosity])
    logging.getLogger('amaxa').handlers[:] = [logging.StreamHandler()]

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

    (ex, errors) = loader.load_extraction_operation(config, context)

    if ex is not None:
        ex.execute()
    else:
        print('Unable to execute operation due to the following errors:\n {}'.format('\n'.join(errors)))
        return -1

    return 0