import argparse
import logging
import yaml
import json
import os.path
from .loader import CredentialLoader, ExtractionOperationLoader, LoadOperationLoader, StateLoader, load_file, save_state
from . import amaxa

def main():
    a = argparse.ArgumentParser()

    a.add_argument('config', type=argparse.FileType('r'))
    a.add_argument('-c', '--credentials', required=True, dest='credentials', type=argparse.FileType('r'))
    a.add_argument('-l', '--load', action='store_true')
    a.add_argument('-s', '--use-state', dest='use_state', type=argparse.FileType('r'))
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
    credential_loader = CredentialLoader(load_file(args.credentials))
    credential_loader.load()

    if credential_loader.errors:
        print('The supplied credentials were not valid: {}'.format('\n'.join(credential_loader.errors)))
        return -1

    config = load_file(args.config)

    if args.load:
        operation_loader = LoadOperationLoader(config, credential_loader.result, use_state=args.use_state is not None)
    else:
        operation_loader = ExtractionOperationLoader(config, credential_loader.result)

    operation_loader.load()
    if operation_loader.errors:
        print('Errors occured during load of the operation: {}'.format('\n'.join(operation_loader.errors)))
        return -1
    
    ex = operation_loader.result

    if args.use_state:
        state_file = load_file(args.use_state)
        state_loader = StateLoader(state_file, ex)
        state_loader.load()
        if state_loader.errors:
            print('Errors occured during load of the state file: {}'.format('\n'.join(state_loader.errors)))
            return -1

    ret = ex.run()

    if ret != 0 and ex.global_id_map:
        # Save the operation state.
        json_mode = args.config.name.endswith('json')
        state_file = open(
            os.path.splitext(args.config.name)[0] + '.state' + ('.json' if json_mode else '.yaml'),
            'w'
        )
        state_file.write(save_state(ex, json_mode))

    return ret