################
# Python 3.5.2 #
################

import configparser
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
    def connected_func(*args):
        conn = pg2.connect(cursor_factory=pg2_extras.RealDictCursor, **get_connection_params())

        with conn, conn.cursor() as cur:
            result = func(cur, *args)

        conn.close()
        return result

    return connected_func
