################
# Python 3.5.2 #
################

import sys
import os
import json
import argparse
import re

metadata_name = 'smart-trap-metadata.json'

def update_traps(args):
    api_key = args.api_key
    files = args.file

    with open(metadata_name, 'r+') as metadata_f:
        metadata = json.load(metadata_f)

        if api_key not in metadata:
            raise ValueError('API key does not exist.')

        traps = metadata[api_key]['traps']
        other_traps = [trap for key in metadata for trap in metadata[key]['traps'] if key != api_key]

        for filename in files:
            with open(filename, 'r') as json_f:
                js = json.load(json_f)

                for trap_wrapper in js['traps']:
                    trap_id = trap_wrapper['Trap']['id']

                    if trap_id in other_traps:
                        raise ValueError('Trap {} already exists under a different API key.'.format(trap_id))

                    elif trap_id not in traps:
                        traps.append(trap_id)

        overwrite_json(metadata, metadata_f)
        print('Success.')

def change_key(args):
    old_key = args.old_key
    new_key = args.new_key

    if old_key == new_key:
        print('Notice: New key matches old key - no change.')

    else:
        with open(metadata_name, 'r+') as metadata_f:
            metadata = json.load(metadata_f)

            if old_key not in metadata:
                raise ValueError('Old key does not exist.')
            elif new_key in metadata:
                raise ValueError('New key already exists.')
            else:
                metadata[new_key] = metadata.pop(old_key)
                metadata[new_key]['prev_keys'].append(old_key)

                overwrite_json(metadata, metadata_f)
                print('Success.')

def add_key(args):
    new_key = args.new_key
    prefix = args.prefix
    entity = args.entity

    with open(metadata_name, 'r+') as metadata_f:
        metadata = json.load(metadata_f)
        prefixes = [metadata[key]['prefix'] for key in metadata]

        if new_key in metadata:
            raise ValueError('Key already exists.')
        elif prefix in prefixes:
            raise ValueError('Prefix already exists.')
        else:
            metadata[new_key] = {
                'traps': [],
                'entity': entity,
                'prefix': prefix,
                'ordinals': {},
                'locations': [],
                'prev_keys': []
            }

            overwrite_json(metadata, metadata_f)
            print('Success.')

def overwrite_json(js, f):
    f.seek(0)
    f.truncate()
    json.dump(js, f, indent=4)

def parse_args():
    parser = argparse.ArgumentParser(description='Provides a set of tools to update smart trap persisent metadata.')
    subparsers = parser.add_subparsers(title='subcommands', help='Add the subcommand name followed by -h for specific help on each.')

    parser_ut = subparsers.add_parser('update-traps',
        help='Adds any new traps found in a list of files to an API key\'s trap list.')
    parser_ut.add_argument('api_key', type=api_key, help='The API key to update.')
    parser_ut.add_argument('file', nargs='+', help='The file(s) to search for new traps.')
    parser_ut.set_defaults(func=update_traps)

    parser_ck = subparsers.add_parser('change-key', help='Changes the API key associated with a particular set of metadata.')
    parser_ck.add_argument('old_key', type=api_key, help='The old API key.')
    parser_ck.add_argument('new_key', type=api_key, help='The new API key.')
    parser_ck.set_defaults(func=change_key)

    parser_ak = subparsers.add_parser('add-key', help='Adds a new key and associated set of metadata.')
    parser_ak.add_argument('new_key', type=api_key, help='The new API key.')
    parser_ak.add_argument('entity', type=non_empty,
        help='The entity, organization, or authority associated with this key.')
    parser_ak.add_argument('prefix', type=non_empty,
        help='The prefix to use for collection and sample IDs associated with this key.')
    parser_ak.set_defaults(func=add_key)

    args = parser.parse_args()

    return args

def api_key(string):
    if not re.match('[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', string):
        raise argparse.ArgumentTypeError('Invalid API key.')

    return string

def non_empty(string):
    if not string:
        raise argparse.ArgumentTypeError('Argument cannot be empty.')

    return string

if __name__ == '__main__':
    args = parse_args()
    args.func(args)

