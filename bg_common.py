"""
Contains functions used at multiple steps in the pipeline.

get_connection_params -- Get the database connection parameters.
run_with_connection -- Run a function with a database connection.
make_datetime -- Make a datetime object from a full timestamp string.
make_date -- Make a date object from a full timestamp string.
parse_date -- Try to make a datetime object from an arbitrary string.

This module requires at least Python 3.5.
"""

import argparse
import configparser
import datetime as dt
from functools import wraps

import psycopg2 as pg2
import psycopg2.extras as pg2_extras

config_file = 'db_config.ini'


def get_connection_params():
    """Return a dict-like object with database connection parameters.

    Parses the database config file to get connection parameters.
    """
    config = configparser.ConfigParser()
    config.read(config_file)

    return config['database']


def run_with_connection(func):
    """Run a function with a database connection.

    This function is a decorator that sets up a database connection and
    gives the called function a cursor through that connnection.  Note:
    This adds the 'cur' parameter to the beginning of func's parameter
    list, therefore any function using this decorator must include an
    extra cursor parameter at the beginning of its parameter list but
    omit this parameter when being called.  All other parameters must be
    provided as keyword arguments.
    """
    @wraps(func)
    def connected_func(**kwargs):
        conn = pg2.connect(cursor_factory=pg2_extras.RealDictCursor, **get_connection_params())

        with conn, conn.cursor() as cur:
            result = func(cur, **kwargs)

        conn.close()
        return result

    return connected_func


def make_datetime(string):
    """Make a datetime object from a full timestamp string.

    Attempts to interpret a string as a full timestamp string and create
    a datetime object from that string, returning None if the string
    represents an empty date.
    """
    if string == '0000-00-00 00:00:00':
        return None
    else:
        return dt.datetime.strptime(string, '%Y-%m-%d %H:%M:%S')


def make_date(string):
    """Make a date object from a full timestamp string.

    Attempts to interpret a string as a full timestamp string and create
    a date object from that string, dropping the time information.
    Returns None if the string represents an empty date.
    """
    date_time = make_datetime(string)

    if date_time:
        return date_time.date()
    else:
        return None


def parse_date(string):
    """Try to make a datetime object from an arbitrary string.

    Tries to interpret a string as any one of three date/datetime
    formats and raises an argparse error if unsuccessful in all three
    attempts.  Used as an argparse type check.
    """
    for fmt in ('%Y-%m-%dT%H-%M-%S', '%Y-%m-%dT%H-%M', '%Y-%m-%d'):
        try:
            return dt.datetime.strptime(string, fmt)
        except ValueError:
            pass

    raise argparse.ArgumentTypeError('Acceptable time formats ("T" is literal): "YYYY-MM-DD", '
                                     '"YYYY-MM-DDTHH-MM", "YYYY-MM-DDTHH-MM-SS"')
