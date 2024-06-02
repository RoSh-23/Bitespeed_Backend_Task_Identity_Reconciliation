from sqlalchemy import create_engine
from flask import g
import os

username = os.environ.get('DB_USER')
password = os.environ.get('DB_PWD')
hostname = os.environ.get('DB_HOSTNAME')
database_name = 'bitespeed_problem_db'

def get_db_connection():
    # Create a connection string
    connection_string = f'mysql+mysqlconnector://' + username + \
        ':' + password + '@' + hostname + '/' + database_name
    # create engine
    engine = create_engine(connection_string)

    if 'db_connection' not in g:
        # create connection
        g.db_connection = engine.connect()

    # return the connection
    return g.db_connection


def close_db_connection(e=None):
    db_connection = g.pop('db_connection', None)

    if db_connection is not None:
        db_connection.close()