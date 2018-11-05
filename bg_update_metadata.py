################
# Python 3.5.2 #
################

import argparse
import json
import re

import bg_common as com


def parse_args():
    # Parse the command line arguments.

    parser = argparse.ArgumentParser(description='Provides a set of tools to update smart trap '
                                                 'database data.')
    subparsers = parser.add_subparsers(title='subcommands',
                                       help='Add a subcommand name followed by -h for specific '
                                            'help on it.')

    # update-traps parser.
    parser_ut = subparsers.add_parser('update-traps',
                                      help="Adds any new traps found in a list of files to "
                                           "an API key's trap list.")
    parser_ut.add_argument('api_key', type=api_key, help='The API key to update.')
    parser_ut.add_argument('file', nargs='+', help='The file(s) to search for new traps.')
    parser_ut.set_defaults(func=update_traps)

    # change-key parser.
    parser_ck = subparsers.add_parser('change-key',
                                      help='Changes the API key associated with a particular '
                                           'provider.')
    parser_ck.add_argument('old_key', type=api_key, help='The old API key.')
    parser_ck.add_argument('new_key', type=api_key, help='The new API key.')
    parser_ck.set_defaults(func=change_key)

    # add-provider parser.
    parser_ap = subparsers.add_parser('add-provider',
                                      help='Adds a new data provider to the database.')
    parser_ap.add_argument('new_key', type=api_key, help="The provider's API key.")
    parser_ap.add_argument('prefix', type=non_empty,
                           help='The prefix to use for collection and sample IDs associated with '
                                'this provider.')
    parser_ap.add_argument('study_tag', type=non_empty,
                           help='The VBcv study tag associated with this provider.')
    parser_ap.add_argument('study_tag_number', type=non_empty,
                           help='The VBcv study tag term accession number associated with '
                                'this provider.')
    parser_ap.add_argument('obfuscate', type=non_empty,
                           help="Whether to obfuscate this provider's GPS data. "
                                "Please provide a boolean value: yes/no, true/false, 1/0.")
    parser_ap.add_argument('-f', '--file', dest='update_traps_file',
                           help="Add the traps within the given file to the provider's metadata. "
                                "Equivalent to running update-traps with the file.")

    parser_ak_info = parser_ap.add_argument_group(title='Contact information options',
                                                  description='Must provide at least one full name'
                                                              ' and email address.')
    parser_ak_info.add_argument('--on', '--org-name', dest='org_name', type=non_empty,
                                help='The name of the organization associated with this provider.')
    parser_ak_info.add_argument('--oe', '--org-email', dest='org_email', type=non_empty,
                                help='The email address of the organization '
                                     'associated with this provider.')
    parser_ak_info.add_argument('--ou', '--org-url', dest='org_url', type=non_empty,
                                help="The URL of the organization's website.")
    parser_ak_info.add_argument('--cfn', '--contact-first-name', dest='contact_first_name',
                                type=non_empty, help='The first name of the person/contact '
                                                     'associated with this provider.')
    parser_ak_info.add_argument('--cln', '--contact-last-name', dest='contact_last_name',
                                type=non_empty, help='The last name of the person/contact '
                                                     'associated with this provider.')
    parser_ak_info.add_argument('--ce', '--contact-email', dest='contact_email', type=non_empty,
                                help='The email address of the person/contact '
                                     'associated with this provider.')
    parser_ap.set_defaults(func=add_provider)

    args = parser.parse_args()

    if not hasattr(args, 'func'):
        parser.error('Must provide a subcommand.')

    if args.func == add_provider:
        if not (args.org_name or args.contact_first_name or args.contact_last_name):
            parser.error('Must provide either an organization name or a contact name.')
        if not (args.org_email or args.contact_email):
            parser.error('Must provide at least one email address.')

    return args


@com.run_with_connection
def update_traps(cur, api_key, file):
    # Search a file for new traps and add any that are found to the
    # given API key.  Note: Omit the 'cur' argument when calling.

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


@com.run_with_connection
def change_key(cur, old_key, new_key):
    # Change a provider's API key.
    # Note: Omit the 'cur' argument when calling.

    if old_key == new_key:
        print('Notice: New key matches old key - no change.')

    else:
        sql = 'UPDATE providers SET api_key = %s WHERE api_key = %s'
        cur.execute(sql, (new_key, old_key))

        if not cur.rowcount:
            raise ValueError('Old key does not exist.')


@com.run_with_connection
def add_provider(cur, prefix, new_key, study_tag, study_tag_number, obfuscate, org_name=None,
                 org_email=None, org_url=None, contact_first_name=None, contact_last_name=None,
                 contact_email=None):
    # Add a new provider to the database.
    # Note: Omit the 'cur' argument when calling.

    sql = ('INSERT INTO providers (prefix, api_key, org_name, org_email, org_url, '
           'contact_first_name, contact_last_name, contact_email, '
           'study_tag, study_tag_number, obfuscate) '
           'VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);')
    cur.execute(sql, [prefix, new_key, org_name, org_email, org_url, contact_first_name,
                      contact_last_name, contact_email, study_tag, study_tag_number, obfuscate])


def api_key(string):
    # Check whether the argument is a valid API key.

    if not re.match('[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', string):
        raise argparse.ArgumentTypeError('Invalid API key.')

    return string


def non_empty(string):
    # Check whether the argument is not empty.

    if not string:
        raise argparse.ArgumentTypeError('Argument cannot be empty.')

    return string


if __name__ == '__main__':
    args = vars(parse_args())

    # Remove and store arguments that don't get passed to func.
    func = args['func']
    del args['func']
    filename = None

    if 'update_traps_file' in args:
        filename = args['update_traps_file']
        del args['update_traps_file']

    # Run the main function.
    func(**args)

    # Update traps after adding a key if specified.
    if func == add_provider and filename:
        update_traps(api_key=args['api_key'], file=filename)

    print('Success.')
