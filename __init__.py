import os
import logging
import requests
import json
import pandas as pd
from bs4 import BeautifulSoup
from unicodedata import normalize
from modules.designers import get_designers
from config.config import designer_url
from modules.db import db_init, save_list_network_to_db
from config.db_connection import df_to_db, run_query

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(getattr(logging, os.getenv("LOG_LEVEL", "INFO")))


db_init()
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


def games_from_files():
    path_to_json = (
        r"C:\Users\NB-FSILVA\python_personal\bgg_api_extraction\all_games_json"
    )
    json_files = [
        pos_json for pos_json in os.listdir(path_to_json) if pos_json.endswith(".json")
    ]

    boardgame = []
    mechanic = []
    designer = []

    for js in json_files:
        with open(os.path.join(path_to_json, js)) as json_file:
            json_text = json.load(json_file)

            boardgame.append(boardgame_from_json(json_text))
            mechanic.append(bg_mechanic_from_json(json_text))
            designer.append(bg_designer_from_json(json_text))

    df_to_db(pd.concat(boardgame), "boardgame", "boardgames")
    df_to_db(
        pd.concat(mechanic), "bg_x_mechanic", "boardgames", ["game_id", "mechanic_id"]
    )
    df_to_db(
        pd.concat(designer), "bg_x_designer", "boardgames", ["game_id", "designer_id"]
    )


def boardgame_from_json(json_game):
    data = {}
    # ID
    data["id"] = int(json_game["@id"])
    # NAME
    names = json_game["name"]
    if isinstance(names, list):
        data["name"] = [x["@value"] for x in names if x["@type"] == "primary"][0]
    else:
        data["name"] = names["@value"]
    # WEIGHT
    data["weight"] = float(
        json_game["statistics"]["ratings"]["averageweight"]["@value"]
    )
    # RATING
    data["rating"] = float(json_game["statistics"]["ratings"]["average"]["@value"])
    # YEAR
    data["year"] = int(json_game["yearpublished"]["@value"])
    # TYPE
    data["type"] = json_game["@type"]
    # MINPLAYERS
    data["minplayers"] = int(json_game["minplayers"]["@value"])
    data["maxplayers"] = int(json_game["maxplayers"]["@value"])
    data["age"] = int(json_game["minage"]["@value"])
    data["minplaytime"] = int(json_game["minplaytime"]["@value"])
    data["maxplaytime"] = int(json_game["maxplaytime"]["@value"])
    data["rating_users"] = int(
        json_game["statistics"]["ratings"]["usersrated"]["@value"]
    )
    data["weight_users"] = int(
        json_game["statistics"]["ratings"]["numweights"]["@value"]
    )

    return pd.json_normalize(data)


def bg_mechanic_from_json(json_game):
    data = {}
    # ID
    data["game_id"] = int(json_game["@id"])
    # Mechanics
    data["mechanics"] = {
        int(x["@id"]): x["@value"].replace("'s", "s").replace("'", '"')
        for x in json_game["link"]
        if x["@type"] == "boardgamemechanic"
    }
    for k, v in data["mechanics"].items():
        sql = """INSERT INTO boardgames.mechanics (id, name)
                VALUES (%s, %s)
                ON CONFLICT (id) DO NOTHING
            """
        run_query(sql, execute_only=True, parameters=(k, v))

    mechanic_ids = data["mechanics"].keys()
    return pd.DataFrame(
        {
            "game_id": [(data["game_id"])] * len(mechanic_ids),
            "mechanic_id": mechanic_ids,
        }
    )


def bg_designer_from_json(json_game):
    data = {}
    json_game.keys()
    # ID
    data["game_id"] = int(json_game["@id"])
    # Designers
    data["designer"] = {
        int(x["@id"]): normalize("NFKC", x["@value"])
        for x in json_game["link"]
        if x["@type"] == "boardgamedesigner"
    }
    for k, v in data["designer"].items():
        sql = """INSERT INTO boardgames.designer (id, name)
                VALUES (%s, %s)
                ON CONFLICT (id) DO NOTHING
            """
        run_query(sql, execute_only=True, parameters=(k, v))

    designer_ids = data["designer"].keys()
    return pd.DataFrame(
        {
            "game_id": [(data["game_id"])] * len(designer_ids),
            "designer_id": designer_ids,
        }
    )
