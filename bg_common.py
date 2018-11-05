################
# Python 3.5.2 #
################

import argparse
import configparser
import datetime as dt
from functools import wraps

import psycopg2 as pg2
import psycopg2.extras as pg2_extras

config_file = 'config.ini'


def get_connection_params():
    # Return a dict-like object containing the
    # database connection parameters.

    config = configparser.ConfigParser()
    config.read(config_file)

    return config['database']


def run_with_connection(func):
    # Set up a database connection and give the called function
    # a cursor.  Note: This adds the 'cur' parameter to the
    # beginning of func's parameter list.

    @wraps(func)
    def connected_func(**kwargs):
        conn = pg2.connect(cursor_factory=pg2_extras.RealDictCursor, **get_connection_params())

        with conn, conn.cursor() as cur:
            result = func(cur, **kwargs)

        conn.close()
        return result

    return connected_func


def make_datetime(string):
    # Attempt to create a datetime object from a string,
    # returning None if the string represents an empty date

    if string == '0000-00-00 00:00:00':
        return None
    else:
        return dt.datetime.strptime(string, '%Y-%m-%d %H:%M:%S')


def make_date(string):
    # Attempt to create a date object from a string.

    date_time = make_datetime(string)

    if date_time:
        return date_time.date()
    else:
        return None


def parse_date(string):
    # Try to create a datetime from a string and raise
    # an argparse error if unsuccessful.

    for fmt in ('%Y-%m-%dT%H-%M-%S', '%Y-%m-%dT%H-%M', '%Y-%m-%d'):
        try:
            return dt.datetime.strptime(string, fmt)
        except ValueError:
            pass

    raise argparse.ArgumentTypeError('Acceptable time formats ("T" is literal): "YYYY-MM-DD", '
                                     '"YYYY-MM-DDTHH-MM", "YYYY-MM-DDTHH-MM-SS"')
