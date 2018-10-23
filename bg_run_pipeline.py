################
# Python 3.5.2 #
################

import datetime as dt
import os
import subprocess
import time

from bg_download_data import download_data
from bg_update_metadata import update_traps
from bg_json_parser import parse_json
from bg_common import run_with_connection


def main():
    providers = get_providers()
    end_time = dt.datetime.combine(dt.date.today() - dt.timedelta(days=1), dt.time())
    extras_dir = './extras/'

    if not os.path.exists(extras_dir):
        os.makedirs(extras_dir)

    for provider in providers:
        json_output = provider['prefix'] + '_data.json'

        if not provider['last_download']:
            start_time = dt.datetime(2000, 1, 1)
        else:
            start_time = provider['last_download']

        # If we didn't already get data from this provider today
        if end_time > start_time:
            # Send a message if we got data from this provider within the last month
            if end_time - start_time < dt.timedelta(days=31):
                print('Notice: Last download for prefix \'{}\' occurred less than a month ago: {}.\nContinuing in 5 seconds.'
                      .format(provider['prefix'], provider['last_download']))
                time.sleep(5)

            # If the data file doesn't already exist, download the data
            if not os.path.isfile(json_output):
                download_data(stdscr=None, api_key=provider['api_key'], start_time=start_time, end_time=end_time, output=json_output)

            # Add any new traps that might exist to the database
            update_traps(api_key=provider['api_key'], file=[json_output])

            # Parse the JSON and return the metadata of successful projects, if any
            projects = parse_json(files=[json_output], split_years=True)

            # Update the last download time
            update_last_download(prefix=provider['prefix'], time=end_time)

            for project in projects:
                # Define filenames
                project_id = '{}_{}'.format(project['prefix'], project['year'])
                project_dir = './' + project_id + '/'
                interchange_name = project_id + '.pop'
                config_name = project_id + '.config'
                investigation_name = project_id + '.inv'

                # Create ISA sheets
                subprocess.run(['perl', 'PopBio-interchange-format/PopBioWizard.pl', '--file', interchange_name, '--config', config_name,
                                '--sample', '--collection', '--species', '--bg-counter'])

                # Move sheets to the project directory
                if not os.path.exists(project_dir):
                    os.makedirs(project_dir)

                perl_outputs = ['a_collection.csv', 'a_species.csv', 's_sample.csv']

                for output in perl_outputs:
                    os.rename(output, project_dir + output)

                os.rename(investigation_name, project_dir + 'i_investigation.csv')

                # Move extra files to the extras folder
                os.rename(interchange_name, extras_dir + interchange_name)
                os.rename(config_name, extras_dir + config_name)

            # Move the JSON output file to the extras folder
            os.rename(json_output, extras_dir + json_output)


# Get the prefixes and API keys for providers that have an API key
# Note: Call as 'get_providers()'; 'cur' is added by the decorator
@run_with_connection
def get_providers(cur):
    sql = 'SELECT prefix, api_key, last_download FROM providers WHERE api_key IS NOT NULL'
    cur.execute(sql)
    rows = cur.fetchall()

    return rows


# Update the last download time for a provider
# Note: Call as 'update_last_download(prefix=prefix, time=time)'; 'cur' is added by the decorator
@run_with_connection
def update_last_download(cur, prefix, time):
    sql = 'UPDATE providers SET last_download = %s WHERE prefix = %s'
    cur.execute(sql, (time, prefix))


main()
