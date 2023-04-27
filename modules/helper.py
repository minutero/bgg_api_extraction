import os
import time
import logging
from modules.boardgame import boardgame
from modules.db import check_exists_db

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(getattr(logging, os.getenv("LOG_LEVEL", "INFO")))


def save_games(list_ids: list):
    list_games = [boardgame(id=ind_id) for ind_id in list_ids]
    list_games = [
        game for game in list_games if not check_exists_db(id=game.id, check_only=True)
    ]
    logger.info(
        f"From all {len(list_ids)} games only {len(list_games)} are not in the database"
    )
    for game in list_games:
        game.get_boardgame_information()
        game.save_to_db()
        logger.info(f"{game.name} inserted in database")
        time.sleep(2)

