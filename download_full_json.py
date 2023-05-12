import json
import time
import logging
import os
from modules.api_request import bgg_api_call
from modules.db import run_query

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(getattr(logging, os.getenv("LOG_LEVEL", "INFO")))


def get_all_json():
    games_ready = os.listdir(
        r"C:\Users\NB-FSILVA\python_personal\bgg_api_extraction\all_games_json"
    )
    start = max([int(x[5:11]) for x in games_ready], default=0)
    list_game_ids = run_query(
        f"select id from boardgame where id > {start} order by id", conn_type="sqlite"
    )["id"].values
    i = 0
    len_game = len(list_game_ids)
    logger.info(f"Downloading {len_game} games")
    for game_id in list_game_ids:
        i += 1
        json_response = bgg_api_call("thing", game_id)
        filepath = r"C:\Users\NB-FSILVA\python_personal\bgg_api_extraction\all_games_json\game_{i}.json".format(
            i=str(game_id).zfill(6)
        )
        if i % 1000 == 0:
            logger.info(f"Game {i} downloaded from {len_game} in total")
        with open(filepath, "w") as fp:
            json.dump(json_response, fp)
        time.sleep(2)

    return True


end_condition = False
while not (end_condition):
    try:
        end_condition = get_all_json()
    except Exception as e:
        logger.error("Error in bgg_api_call")
        logger.info(e)
        time.sleep(120)
        continue
