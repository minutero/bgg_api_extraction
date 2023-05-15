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

    df_designer_download = run_query(
        f"""with designer_in_list as (
                select designer_id
                from boardgames.bg_x_designer
                where game_id in ({','.join(games)})
                )
            select dl.designer_id,
                d.name,
                count(distinct game_id)
            from boardgames.bg_x_designer db
            inner join boardgames.designer d on db.designer_id = d.id
            inner join designer_in_list dl on db.designer_id = dl.designer_id
            group by 1,2
            having count(distinct game_id) <= {str(maximum_in_db_to_load)}""",
    )

    get_games_from_designer(
        df_designer_download.designer_id.to_list(), df_designer_download.name.to_list()
    )
