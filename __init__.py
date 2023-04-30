import os
import logging
import requests
from bs4 import BeautifulSoup
from modules.designers import get_designers, get_games_from_designer
from modules.config import database, designer_url
from modules.db import db_init, save_list_network_to_db
from config.db_connection import run_query

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(getattr(logging, os.getenv("LOG_LEVEL", "INFO")))


db_init(database)
get_designers(designer_url)

url = "https://boardgamegeek.com/browse/boardgame"
req = requests.get(url)
html = req.content
bs = BeautifulSoup(html, features="html.parser")
games_top100 = bs.table.findAll(
    lambda tag: tag.name == "a" and tag.has_attr("href") and tag.has_attr("class")
)
list_games_id = [k["href"].split("/")[2] for k in games_top100]
save_list_network_to_db(list_games_id)
