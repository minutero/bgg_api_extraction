import os
import logging
import time
import pandas as pd
from config.db_connection import run_query, create_connection
from config.config import columns
from modules.boardgame import boardgame
from typing import (
    List,
    Optional,
)

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(getattr(logging, os.getenv("LOG_LEVEL", "INFO")))


def db_init():
    """Creates the database if it does not exist"""
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
    run_query(query, execute_only=True)


def save_games(list_ids: list, verbose=False):
    already_db = (
        run_query(
            f"select id from boardgames.boardgame where id in ({','.join(list_ids)})"
        )
        .id.apply(str)
        .to_list()
    )
    list_games = [
        boardgame(id=ind_id) for ind_id in list_ids if ind_id not in already_db
    ]
    if verbose:
        logger.info(
            f"From all {len(list_ids)} games {len(list_games)} are going to be inserted in the database"
        )
    if len(list_games):
        list_game_list = []
        list_designer_list = []
        list_bxd = []
        list_mechanic_list = []
        list_bxm = []
        for game in list_games:
            try:
                game.get_boardgame_information()
                if verbose:
                    logger.info(f"Game {game.name}({game.id}) was correctly obtained")
                time.sleep(2)
            except:
                logger.warning(
                    f"Game {list_games.index(game)+1} in list with id {game.id} could not be obtained from API. Trying next game."
                )
                continue
            game_info = game.to_list_db()
            list_game_list.append(game_info["game"])
            list_designer_list.extend(game_info["designer"])
            list_bxd.extend([[game.id, d[0]] for d in game_info["designer"]])
            list_mechanic_list.extend(game_info["mechanic"])
            list_bxm.extend([[game.id, d[0]] for d in game_info["mechanic"]])

        df_boardgame = pd.DataFrame(list_game_list, columns=columns).drop_duplicates()
        df_designer = pd.DataFrame(
            list_designer_list, columns=["id", "name"]
        ).drop_duplicates()
        df_bxd = pd.DataFrame(
            list_bxd, columns=["game_id", "designer_id"]
        ).drop_duplicates()
        df_mechanic = pd.DataFrame(
            list_mechanic_list, columns=["id", "name"]
        ).drop_duplicates()
        df_bxm = pd.DataFrame(
            list_bxm, columns=["game_id", "mechanic_id"]
        ).drop_duplicates()

        conn = create_connection()
        to_postgresql(
            df=df_boardgame,
            con=conn,
            table="boardgame",
            schema="boardgames",
            mode="append",
            insert_conflict_columns=["id"],
            verbose=verbose,
        )
        to_postgresql(
            df=df_designer,
            con=conn,
            table="designer",
            schema="boardgames",
            mode="append",
            insert_conflict_columns=["id"],
            verbose=verbose,
        )
        to_postgresql(
            df=df_bxd,
            con=conn,
            table="bg_x_designer",
            schema="boardgames",
            mode="append",
            insert_conflict_columns=["game_id", "designer_id"],
            verbose=verbose,
        )
        to_postgresql(
            df=df_mechanic,
            con=conn,
            table="mechanics",
            schema="boardgames",
            mode="append",
            insert_conflict_columns=["id"],
            verbose=verbose,
        )
        to_postgresql(
            df=df_bxm,
            con=conn,
            table="bg_x_mechanic",
            schema="boardgames",
            mode="append",
            insert_conflict_columns=["game_id", "mechanic_id"],
            verbose=verbose,
        )
        conn.close()


def to_postgresql(
    df,
    con,
    table: str,
    schema: str,
    mode: str = "append",
    chunksize: int = 200,
    insert_conflict_columns: Optional[List[str]] = None,
    verbose: bool = False,
):
    """Write records stored in a DataFrame into PostgreSQL.

    Args:
    df : pandas.DataFrame
        Pandas DataFrame https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html
    con : pg8000.Connection
        Use pg8000.connect() to use credentials directly or wr.postgresql.connect() to fetch it from the Glue Catalog.
    table : str
        Table name
    schema : str
        Schema name
    mode : str
        Append, overwrite or upsert.
            append: Inserts new records into table.
            overwrite: Drops table and recreates.
            upsert: Perform an upsert which checks for conflicts on columns given by `upsert_conflict_columns` and
            sets the new values on conflicts. Note that `upsert_conflict_columns` is required for this mode.
    index : bool
        True to store the DataFrame index as a column in the table,
        otherwise False to ignore it.
    dtype: Dict[str, str], optional
        Dictionary of columns names and PostgreSQL types to be casted.
        Useful when you have columns with undetermined or mixed data types.
        (e.g. {'col name': 'TEXT', 'col2 name': 'FLOAT'})
    varchar_lengths : Dict[str, int], optional
        Dict of VARCHAR length by columns. (e.g. {"col1": 10, "col5": 200}).
    use_column_names: bool
        If set to True, will use the column names of the DataFrame for generating the INSERT SQL Query.
        E.g. If the DataFrame has two columns `col1` and `col3` and `use_column_names` is True, data will only be
        inserted into the database columns `col1` and `col3`.
    chunksize: int
        Number of rows which are inserted with each SQL query. Defaults to inserting 200 rows per query.
    upsert_conflict_columns: List[str], optional
        This parameter is only supported if `mode` is set top `upsert`. In this case conflicts for the given columns are
        checked for evaluating the upsert.
    insert_conflict_columns: List[str], optional
        This parameter is only supported if `mode` is set top `append`. In this case conflicts for the given columns are
        checked for evaluating the insert 'ON CONFLICT DO NOTHING'.

    Returns:
        None
    """
    if df.empty is True:
        raise Exception("DataFrame cannot be empty.")

    mode = mode.strip().lower()
    allowed_modes = ["append", "overwrite"]
    if mode not in allowed_modes:
        raise KeyError(
            f"Mode not allowed. Please use one of the following {', '.join(allowed_modes)}"
        )
    try:
        with con.cursor() as cursor:
            column_placeholders: str = ", ".join(["%s"] * len(df.columns))
            column_names = [f'"{column}"' for column in df.columns]
            insertion_columns = f"({', '.join(column_names)})"
            upsert_str = ""
            if mode == "append" and insert_conflict_columns:
                conflict_columns = ", ".join(insert_conflict_columns)
                upsert_str = f" ON CONFLICT ({conflict_columns}) DO NOTHING"

            placeholder_parameter_pair_generator = generate_placeholder_parameter_pairs(
                df=df, column_placeholders=column_placeholders, chunksize=chunksize
            )
            for placeholders, parameters in placeholder_parameter_pair_generator:
                sql = f'INSERT INTO "{schema}"."{table}" {insertion_columns} VALUES {placeholders}{upsert_str}'
                cursor.executemany(sql, (parameters,))
            con.commit()
            if verbose:
                logger.info(
                    f"Commit ready. {len(df)} records inserted in {schema}.{table}"
                )
    except Exception as ex:
        con.rollback()
        logger.error(ex)
        raise


def generate_placeholder_parameter_pairs(df, column_placeholders, chunksize):
    parameters = df.values.tolist()
    for i in range(0, len(df.index), chunksize):
        parameters_chunk = parameters[i : i + chunksize]
        chunk_placeholders = ", ".join(
            [f"({column_placeholders})" for _ in range(len(parameters_chunk))]
        )
        flattened_chunk = [value for row in parameters_chunk for value in row]
        yield chunk_placeholders, flattened_chunk
