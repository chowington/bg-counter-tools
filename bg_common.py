################
# Python 3.5.2 #
################

import configparser
from datetime import datetime
from functools import wraps

import psycopg2 as pg2
import psycopg2.extras as pg2_extras

config_file = 'config.ini'


# Returns a dict-like object containing the database connection parameters
def get_connection_params():
    config = configparser.ConfigParser()
    config.read(config_file)

    return config['database']


# Decorator that sets up a database connection and gives the called function a cursor
# Note: This ADDS the 'cur' parameter to the beginning of func's parameter list
def run_with_connection(func):
    @wraps(func)
    def connected_func(**kwargs):
        conn = pg2.connect(cursor_factory=pg2_extras.RealDictCursor, **get_connection_params())

        with conn, conn.cursor() as cur:
            result = func(cur, **kwargs)

        conn.close()
        return result

    return connected_func


# Attempts to create a datetime object from a string
def make_datetime(string):
    if string == '0000-00-00 00:00:00':
        return None
    else:
        return datetime.strptime(string, '%Y-%m-%d %H:%M:%S')


# Attempts to create a date object from a string
def make_date(string):
    date_time = make_datetime(string)

    if date_time:
        return date_time.date()
    else:
        return None
