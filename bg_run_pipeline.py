"""
Runs the full BG-Counter Tools pipeline.

This includes getting a list of smart trap data providers from the
database and pulling new raw JSON data, adding new traps found in this
data to the database, parsing the JSON to create interchange format
files, and finally converting those into ISA-Tab files.

The final ISA-Tab files are dumped into directories corresponding to
each "project" that was created, where a project consists of data from a
single provider within a single year.  The directories will be named
[provider prefix]_[year].  Four ISA-Tabs are created for each project.
Additionally, there are intermediate files that are created in the
process; for quality assurance purposes these are dumped into the
'extras' folder after the pipeline has finished for each provider.  To
prevent confusion, all directories should be moved or deleted before the
next run of this script.

By default, the script will read from the database the ending date that
was used the last time the script was run for each provider.  It will
use this as the new starting date for the provider.  The ending date, by
default, will be the beginning of the day before the script is being
run.

It is possible (and designed such) that the script may halt in an error
during execution.  It is also possible that some providers will
successfully finish the pipeline while others will not.  Due to the
stateful nature of the system, this script contains a set of safeguards
for preventing the unnecessary changing of database data due to a rerun
of the pipeline on a provider that already finished the pipeline in a
previous run.  The script will not run for a provider that has an
ending date that is identical to the ending date that is being used for
this run.  Thus it will not rerun the pipeline for a provider that
finished the pipeline within the current calendar day.  If the last run
occurred on a previous day, then it will be the user's responsibility to
specify which providers to run the pipeline on using the --include or
--exclude options.

For usage information, run with -h.

This script requires at least Python 3.5.
"""

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
    """Parse the command line arguments and return an args namespace."""
    parser = argparse.ArgumentParser(description='Runs the full BG-Counter Tools pipeline.')

    parser.add_argument('-s', '--start-time', type=com.parse_date,
                        help='Beginning of the target timeframe. Acceptable time formats ("T" is '
                             'literal): "YYYY-MM-DD", "YYYY-MM-DDTHH-MM", "YYYY-MM-DDTHH-MM-SS"')
    parser.add_argument('-e', '--end-time', type=com.parse_date,
                        help='End of the target timeframe. Same acceptable formats as above.')
    parser.add_argument('--preserve-metadata', action='store_true',
                        help="Don't change the metadata in the database other than adding new "
                             "traps, which is required for the pipeline to work.")
    parser.add_argument('--rerun', action='store_true',
                        help='Run pipeline even for providers that have already been run recently.'
                             ' WARNING: This does not revert any database changes that were made'
                             ' that will affect the output.')

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
                 preserve_metadata=False, rerun=False):
    """Run the full BG-Counter Tools pipeline.

    Optional arguments:
    include -- A list of prefixes to run on.  If passed None, then all
        prefixes will be run.
    exclude -- A list of prefixes to skip.
    start_time -- A datetime object representing the beginning of the
        timeframe to get data over.
    end_time -- A datetime object representing the end of the timeframe
        to get data over.
    preserve_metadata -- A boolean signaling whether to leave the
        database data unchanged (apart from adding new traps).
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
        if end_time > start_time or rerun:
            # Send a message if we got data from this provider
            # within the last month.
            if end_time - start_time < dt.timedelta(days=31) and not rerun:
                print("Notice: Last download for prefix '{}' occurred less than a month ago: {}."
                      "\nContinuing in 5 seconds."
                      .format(provider['prefix'], provider['last_download']))
                time.sleep(5)

            # If the data file doesn't already exist, download the data.
            if not os.path.isfile(json_output):
                download_data(stdscr=None, api_key=provider['api_key'], start_time=start_time,
                              end_time=end_time, output=json_output)
            else:
                print('Notice: Using raw trap data from file {}.\nContinuing in 5 seconds.'
                      .format(json_output))
                time.sleep(5)

            # Add any new traps to the database.
            update_traps(api_key=provider['api_key'], file=[json_output])

            # Parse the JSON and return the metadata
            # of successful projects, if any.
            projects = parse_json(files=[json_output], split_years=True, check_locations=True,
                                  preserve_metadata=preserve_metadata)

            # Update the last download time.
            if not preserve_metadata:
                update_last_download(prefix=provider['prefix'], time=end_time)

            for project in projects:
                # Define filenames.
                project_id = '{}_{}'.format(project['prefix'], project['year'])
                project_dir = './' + project_id + '/'
                interchange_name = project_id + '_saf.csv'
                config_name = project_id + '_config.yaml'

                # Create the project directory.
                if not os.path.exists(project_dir):
                    os.makedirs(project_dir)

                # Create ISA-Tabs.
                subprocess.run(check=True, args=[
                    'perl', 'PopBio-interchange-format/PopBioWizard.pl', '--file',
                    interchange_name, '--config', config_name, '--output-directory', project_dir,
                    '--isatab'
                ])

                # Move extra files to the extras folder.
                os.rename(interchange_name, extras_dir + interchange_name)
                os.rename(config_name, extras_dir + config_name)

            # Move the JSON output file to the extras folder.
            os.rename(json_output, extras_dir + json_output)

        else:
            print('Skipping provider {} - data already pulled today.'.format(provider['prefix']))


@com.run_with_connection
def get_providers(cur):
    """Get data for providers that have an API key from the database.

    Note: Omit the 'cur' argument when calling.
    """
    sql = 'SELECT prefix, api_key, last_download FROM providers WHERE api_key IS NOT NULL'
    cur.execute(sql)
    rows = cur.fetchall()

    return rows


@com.run_with_connection
def update_last_download(cur, prefix, time):
    """Update the last download time for a provider.

    Arguments:
    prefix -- The string which is the prefix of the desired data
        provider.
    time -- A datetime object representing the end of the timeframe over
        which the data was collected.

    Note: Omit the 'cur' argument when calling and provide other
    arguments as keyword args.
    """
    sql = 'UPDATE providers SET last_download = %s WHERE prefix = %s'
    cur.execute(sql, (time, prefix))


if __name__ == '__main__':
    args = vars(parse_args())
    run_pipeline(**args)
