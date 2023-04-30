import os
import logging
from config.db_connection import create_db, run_query
from modules.designers import get_games_from_designer
from modules.config import database
from modules.boardgame import save_games

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(getattr(logging, os.getenv("LOG_LEVEL", "INFO")))


def db_init(db_file=database):
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


def save_list_network_to_db(list_games_id, maximum_in_db_to_load=1):
    games = [str(game_id) for game_id in list_games_id]
    save_games(games)

    query = f"""select distinct designer
                from boardgame
                where id in ({','.join(games)})"""
    df_designers = run_query(query, execute_only=False)

    df_designer_game_count = run_query(
        f"""select designer,count(*) as games_count
        from boardgame b
        group by 1
        having games_count <= {str(maximum_in_db_to_load)}""",
        execute_only=False,
    )
    df_designer_download = df_designers.merge(
        df_designer_game_count, how="inner", on="designer"
    )

    count_designer = len(df_designer_download)
    logger.info("###################################################")
    logger.info(f"Processing {count_designer} designers")
    logger.info("###################################################")
    i = 0
    for designer in df_designer_download.to_dict("records"):
        i += 1
        logger.info("###################################################")
        logger.info(
            f"Designer {str(i).zfill(2)}/{str(count_designer).zfill(2)}: Processing {designer['designer']}"
        )
        logger.info("###################################################")
        get_games_from_designer(designer["designer"])
