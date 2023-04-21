import os
import sqlite3
from sqlite3 import Error
import pandas as pd


def check_exists_db(name=None, id=None):
    try:
        bg = run_query(
            f"SELECT * FROM bgg.boardgame where name = '{name}' or id = {id if id else 0}",
        )
    except:
        print("Database or table not found")
        bg = pd.DataFrame(columns=["id", "name"])

    if name in bg["name"].unique() or id in bg["id"].unique():
        return True, bg
    else:
        return False, bg


def create_db(db_file=os.environ.get("db")):
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


def create_connection(db_file=os.environ.get("db")):
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


def run_query(query: str, read_only: bool = False):
    conn = create_connection()
    c = conn.cursor()
    if read_only:
        c.execute(query)
        return True
    else:
        return pd.read_sql_query(query, c)
