import os
from modules.db import db_init_, run_query
from modules.designers import get_designers, get_games_from_designer
from modules.config import database, designer_url
import logging

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(getattr(logging, os.getenv("LOG_LEVEL", "INFO")))


db_init_(database)
get_designers(designer_url)
query = f"""select distinct designer
            from designers"""
df_designers = run_query(query, execute_only=False)
count_designer = len(df_designers)
i = 0
for designer in df_designers.to_dict("records"):
    i += 1
    logger.info("###################################################")
    logger.info(
        f"Designer {str(i).zfill(2)}/{str(count_designer).zfill(2)}: Processing {designer['designer']}"
    )
    logger.info("###################################################")
    game_count = run_query(
        "select count(1) from boardgame where designer = ?",
        execute_only=False,
        parameters=[designer["designer"]],
    ).loc[0][0]
    if (
        game_count > 4
    ):  # IMPROVEMENT: Find a better way on how to avoid rerunning a designer
        continue
    get_games_from_designer(designer["designer"])
