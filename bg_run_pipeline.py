################
# Python 3.5.2 #
################

import argparse
import datetime as dt
import os
import subprocess
import time

from bg_download_data import download_data
from bg_update_metadata import update_traps
from bg_json_parser import parse_json
import bg_common as com


def parse_args():
    # Parse the command line arguments.

    parser = argparse.ArgumentParser(description='Runs the full BG-Counter Tools pipeline.')

    parser.add_argument('-s', '--start-time', type=com.parse_date,
                        help='Beginning of the target timeframe. Acceptable time formats ("T" is '
                             'literal): "YYYY-MM-DD", "YYYY-MM-DDTHH-MM", "YYYY-MM-DDTHH-MM-SS"')
    parser.add_argument('-e', '--end-time', type=com.parse_date,
                        help='End of the target timeframe. Same acceptable formats as above.')
    parser.add_argument('--preserve-metadata', action='store_true',
                        help="Don't change the metadata in the database other than adding new "
                             "traps, which is required for the pipeline to work.")

    mutex_group = parser.add_mutually_exclusive_group()
    mutex_group.add_argument('-i', '--include', nargs='+',
                             help='Only run the pipeline on the given providers, identified by '
                                  'their prefixes.')
    mutex_group.add_argument('-x', '--exclude', nargs='+',
                             help='Exclude the given providers, identified by their prefixes, from'
                                  ' the pipeline.')

    args = parser.parse_args()

    return args


def run_pipeline(include=None, exclude=None, start_time=None, end_time=None,
                 preserve_metadata=False):
    """Run the full BG-Counter Tools pipeline on providers
    within the database.
    """
    providers = get_providers()
    prefixes = {provider['prefix'] for provider in providers}

    # If providers were specified, check to see if they exist
    # in the database, then keep the desired providers.
    if include:
        specified_prefixes = desired_prefixes = set(include)
    elif exclude:
        specified_prefixes = set(exclude)
        desired_prefixes = prefixes - specified_prefixes
    else:
        specified_prefixes = None

    if specified_prefixes:
        missing = specified_prefixes - prefixes

        if missing:
            raise ValueError('Provider(s) not found in database: ' + ', '.join(missing))

        providers = [provider for provider in providers if provider['prefix'] in desired_prefixes]

    # If the end time was not provided, set it to
    # the beginning of yesterday.
    if not end_time:
        end_time = dt.datetime.combine(dt.date.today() - dt.timedelta(days=1), dt.time())

    extras_dir = './extras/'

    if not os.path.exists(extras_dir):
        os.makedirs(extras_dir)

    for provider in providers:
        # The file that will hold the raw JSON capture data.
        json_output = provider['prefix'] + '_data.json'

        # Get the last download time or set it if it doesn't exist.
        if not start_time:
            if not provider['last_download']:
                start_time = dt.datetime(2000, 1, 1)
            else:
                start_time = provider['last_download']

        # Continue if we didn't already get data
        # from this provider today.
        if end_time > start_time:
            # Send a message if we got data from this provider
            # within the last month.
            if end_time - start_time < dt.timedelta(days=31):
                print("Notice: Last download for prefix '{}' occurred less than a month ago: {}."
                      "\nContinuing in 5 seconds."
                      .format(provider['prefix'], provider['last_download']))
                time.sleep(5)

            # If the data file doesn't already exist, download the data.
            if not os.path.isfile(json_output):
                download_data(stdscr=None, api_key=provider['api_key'], start_time=start_time,
                              end_time=end_time, output=json_output)

            # Add any new traps to the database.
            update_traps(api_key=provider['api_key'], file=[json_output])

            # Parse the JSON and return the metadata
            # of successful projects, if any.
            projects = parse_json(files=[json_output], split_years=True,
                                  preserve_metadata=preserve_metadata)

            # Update the last download time.
            if not preserve_metadata:
                update_last_download(prefix=provider['prefix'], time=end_time)

            for project in projects:
                # Define filenames.
                project_id = '{}_{}'.format(project['prefix'], project['year'])
                project_dir = './' + project_id + '/'
                interchange_name = project_id + '.pop'
                config_name = project_id + '.config'
                investigation_name = project_id + '.inv'

                # Create ISA sheets.
                subprocess.run(['perl', 'PopBio-interchange-format/PopBioWizard.pl', '--file',
                                interchange_name, '--config', config_name, '--sample',
                                '--collection', '--species', '--bg-counter'])

                # Move sheets to the project directory.
                if not os.path.exists(project_dir):
                    os.makedirs(project_dir)

                perl_outputs = ['a_collection.csv', 'a_species.csv', 's_sample.csv']

                for output in perl_outputs:
                    os.rename(output, project_dir + output)

                os.rename(investigation_name, project_dir + 'i_investigation.csv')

                # Move extra files to the extras folder.
                os.rename(interchange_name, extras_dir + interchange_name)
                os.rename(config_name, extras_dir + config_name)

            # Move the JSON output file to the extras folder.
            os.rename(json_output, extras_dir + json_output)


@com.run_with_connection
def get_providers(cur):
    # Get the prefixes and API keys for providers that have an API key.
    # Note: Omit the 'cur' argument when calling.

    sql = 'SELECT prefix, api_key, last_download FROM providers WHERE api_key IS NOT NULL'
    cur.execute(sql)
    rows = cur.fetchall()

    return rows


@com.run_with_connection
def update_last_download(cur, prefix, time):
    # Update the last download time for a provider.
    # Note: Omit the 'cur' argument when calling.

    sql = 'UPDATE providers SET last_download = %s WHERE prefix = %s'
    cur.execute(sql, (time, prefix))


if __name__ == '__main__':
    args = vars(parse_args())
    run_pipeline(**args)
