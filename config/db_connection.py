import sqlite3
from sqlite3 import Error
from modules.config import database
import pandas as pd


def create_db(db_file):
    """create a database connection to a SQLite database"""
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        print(sqlite3.version)
    except Error as e:
        print(e)
    finally:
        if conn:
            conn.close()


def create_connection(db_file=database):
    """create a database connection to the SQLite database
        specified by db_file
    :param db_file: database file
    :return: Connection object or None
    """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Error as e:
        print(e)

    return conn


def run_query(query: str, execute_only: bool = True, parameters=None, db_file=database):
    conn = create_connection(db_file)
    c = conn.cursor()
    if execute_only:
        if parameters:
            c.execute(query, parameters)
        else:
            c.execute(query)
        conn.commit()
        conn.close()
        return True
    else:
        if parameters:
            df = pd.read_sql_query(query, conn, params=parameters)
        else:
            df = pd.read_sql_query(query, conn)
        conn.close()
        return df
