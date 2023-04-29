import os
import logging
import sqlite3
from sqlite3 import Error
import pandas as pd
from modules.api_request import get_from_id, get_from_name
from modules.config import database

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(getattr(logging, os.getenv("LOG_LEVEL", "INFO")))


def db_init_(db_file=database):
    create_db(db_file)
    query = """ CREATE TABLE IF NOT EXISTS boardgame (
                        id integer PRIMARY KEY,
                        name text NOT NULL,
                        designer integer,
                        mechanics text,
                        rating real,
                        year_published integer,
                        type text
                                    );
                CREATE TABLE IF NOT EXISTS designers (
                        id integer PRIMARY KEY,
                        designer text
                                    );"""
    run_query(query)


def check_exists_db(
    name: str = None,
    id: int = None,
    replace_name: bool = True,
    check_only: bool = False,
):
    bg = run_query(
        f"SELECT * FROM boardgame where name = '{name}' or id = {id if id else 0}",
        execute_only=False,
    )

    if bg.empty:
        if check_only:
            return False
        if id is not None:
            bg = pd.json_normalize(get_from_id(id, replace_name))
        elif name is not None:
            bg = pd.json_normalize(get_from_name(name, replace_name))
        else:
            print("Name and ID are empty. Please provide at least one of them")
    else:
        if check_only:
            return True
    return bg.to_dict(orient="records")[0]


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
