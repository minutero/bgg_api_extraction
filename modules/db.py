import os
import logging
from config.db_connection import run_query
from modules.designers import get_games_from_designer
from modules.boardgame import save_games

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


def save_list_network_to_db(list_games_id, maximum_in_db_to_load=1):
    games = [str(game_id) for game_id in list_games_id]
    save_games(games)

    query = f"""select distinct designer
                from boardgame
                where id in ({','.join(games)})"""
    df_designers = run_query(query)

    df_designer_game_count = run_query(
        f"""select designer,count(*) as games_count
        from boardgame b
        group by 1
        having games_count <= {str(maximum_in_db_to_load)}""",
    )
    df_designer_download = df_designers.merge(
        df_designer_game_count, how="inner", on="designer"
    )

    get_games_from_designer(df_designer_download.designer.tolist())
