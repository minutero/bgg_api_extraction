import os
import psycopg2
import sqlite3
from sqlalchemy import create_engine
import pandas as pd


def create_connection(db=None):
    """create a database connection to the Postgres database

    :return: Connection object or None
    """
    conn = None
    try:
        if not db:
            conn = psycopg2.connect(
                database=os.getenv("db_name"),
                host=os.getenv("db_host"),
                user=os.getenv("db_user"),
                password=os.getenv("db_pass"),
                port=os.getenv("db_port"),
            )
        else:
            conn = sqlite3.connect("boardgame.db")
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


def df_to_db(df, table, schema=None, primary=["id"]):
    cols = list(df.columns)
    schema_dot = schema + "." if schema else None
    for row in df.to_dict("records"):
        values = [f"'{x}'" if isinstance(x, str) else str(x) for x in row.values()]
        query = f"""INSERT INTO {schema_dot}{table} ({",".join(cols)})
            VALUES ({",".join(values)})
            ON CONFLICT ({",".join(primary)}) DO NOTHING"""
        run_query(query, execute_only=True)
