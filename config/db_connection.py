import psycopg2
import sqlite3
import pandas as pd
from modules.helper import get_secret


def create_connection(db=None):
    """create a database connection to the Postgres database

    :return: Connection object or None
    """
    conn = None
    credentials = get_secret("rds-postgresql-bgg")
    try:
        if not db:
            conn = psycopg2.connect(
                database=credentials.get("dbInstanceIdentifier"),
                host=credentials.get("host"),
                user=credentials.get("username"),
                password=credentials.get("password"),
                port=credentials.get("port"),
            )
        else:
            conn = sqlite3.connect(db)
        return conn
    except Exception as e:
        print(e)
    return conn


def run_query(query: str, execute_only: bool = False, parameters=None, conn_type=None):
    conn = create_connection(conn_type)
    c = conn.cursor()
    if parameters:
        c.execute(query, parameters)
    else:
        c.execute(query)
    if execute_only:
        conn.commit()
        df = None
    else:
        column_names = [desc[0] for desc in c.description]
        df = pd.DataFrame(c.fetchall(), columns=column_names)
    c.close()
    conn.close()
    return df
