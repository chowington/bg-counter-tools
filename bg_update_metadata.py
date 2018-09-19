################
# Python 3.5.2 #
################

import argparse
import json
import re

from bg_common import run_with_connection


def parse_args():
    parser = argparse.ArgumentParser(description='Provides a set of tools to update smart trap persisent metadata.')
    subparsers = parser.add_subparsers(title='subcommands', help='Add a subcommand name followed by -h for specific help on it.')

    # Update traps parser
    parser_ut = subparsers.add_parser('update-traps',
        help='Adds any new traps found in a list of files to an API key\'s trap list.')
    parser_ut.add_argument('api_key', type=api_key, help='The API key to update.')
    parser_ut.add_argument('file', nargs='+', help='The file(s) to search for new traps.')
    parser_ut.set_defaults(func=update_traps)

    # Change key parser
    parser_ck = subparsers.add_parser('change-key', help='Changes the API key associated with a particular set of metadata.')
    parser_ck.add_argument('old_key', type=api_key, help='The old API key.')
    parser_ck.add_argument('new_key', type=api_key, help='The new API key.')
    parser_ck.set_defaults(func=change_key)

    # Add key parser
    parser_ak = subparsers.add_parser('add-key', help='Adds a new key and associated set of metadata.')
    parser_ak.add_argument('new_key', type=api_key, help='The new API key.')
    parser_ak.add_argument('prefix', type=non_empty,
        help='The prefix to use for collection and sample IDs associated with this key.')

    parser_ak_info = parser_ak.add_argument_group(title='Contact information options',
        description='Must provide at least one name and email address.')
    parser_ak_info.add_argument('-on', '--org-name', type=non_empty,
        help='The name of the organization associated with this key.')
    parser_ak_info.add_argument('-oe', '--org-email', type=non_empty,
        help='The email address of the organization associated with this key.')
    parser_ak_info.add_argument('-cn', '--contact-name', type=non_empty,
        help='The name of the person/contact associated with this key.')
    parser_ak_info.add_argument('-ce', '--contact-email', type=non_empty,
        help='The email address of the person/contact associated with this key.')
    parser_ak.set_defaults(func=add_key)

    args = parser.parse_args()

    if args.func == add_key:
        if not (args.org_name or args.contact_name):
            parser.error('Must provide at least one name.')
        if not (args.org_email or args.contact_email):
            parser.error('Must provide at least one email address.')

    return args


# Note: Call as 'update_traps(api_key, file)'; 'cur' is added by the decorator
@run_with_connection
def update_traps(cur, api_key, file):
    sql = 'SELECT prefix FROM providers WHERE api_key = %s'
    cur.execute(sql, (api_key,))
    row = cur.fetchone()

    if row:
        prefix = row['prefix']
    else:
        raise ValueError('API key does not exist.')

    sql = 'SELECT trap_id FROM traps WHERE prefix = %s'
    cur.execute(sql, (prefix,))

    trap_ids = {row['trap_id'] for row in cur.fetchall()}
    new_traps = []

    for filename in file:
        with open(filename) as json_f:
            js = json.load(json_f)

            for trap_wrapper in js['traps']:
                trap_id = trap_wrapper['Trap']['id']

                if trap_id not in trap_ids:
                    new_traps.append(trap_id)
                    print('New trap: ' + trap_id)

    if new_traps:
        sql = 'INSERT INTO traps VALUES (%s, %s)'
        for trap_id in new_traps:
            cur.execute(sql, (trap_id, prefix))

    else:
        print('No new traps.')


# Note: Call as 'change_key(old_key, new_key)'; 'cur' is added by the decorator
@run_with_connection
def change_key(cur, old_key, new_key):
    if old_key == new_key:
        print('Notice: New key matches old key - no change.')

    else:
        sql = 'UPDATE providers SET api_key = %s WHERE api_key = %s'
        cur.execute(sql, (new_key, old_key))

        if not cur.rowcount:
            raise ValueError('Old key does not exist.')


# Note: Call as 'add_key(prefix, new_key, org_name, org_email, contact_name, contact_email)'; 'cur' is added by the decorator
@run_with_connection
def add_key(cur, prefix, new_key, org_name=None, org_email=None, contact_name=None, contact_email=None):
    sql = 'INSERT INTO providers VALUES (%s, %s, %s, %s, %s, %s);'
    cur.execute(sql, (prefix, new_key, org_name, org_email, contact_name, contact_email))


def api_key(string):
    if not re.match('[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', string):
        raise argparse.ArgumentTypeError('Invalid API key.')

    return string


def non_empty(string):
    if not string:
        raise argparse.ArgumentTypeError('Argument cannot be empty.')

    return string


if __name__ == '__main__':
    args = vars(parse_args())
    func = args['func']
    del args['func']

    func(**args)

    print('Success.')
