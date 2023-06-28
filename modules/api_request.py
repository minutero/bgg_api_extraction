import os
import requests
import xmltodict
import logging
import json
from unicodedata import normalize
from typing import Dict, Mapping
from config.config import url_base

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(getattr(logging, os.getenv("LOG_LEVEL", "INFO")))


def bgg_api_call(
    call_type: str,
    id: int,
    extra_parameters: Mapping = {"stats": 1, "pagesize": 100},
    verbose: bool = False,
) -> Dict:
    """Function to get information in XML from BGG API v2

    Args:
        call_type (str): Command/Type of Element you would like to pull (See https://boardgamegeek.com/wiki/page/BGG_XML_API2 for reference)
        id (int): Value of main parameter (ID,name, query, etc) of the element to pull.
        extra_parameters (Dictionary, optional): Any extra parameters you owuld like to include in the call. Defaults to {"stast": 2, "pagesize": 100}.

    Raises:
        Exception: If Command/Type of Element does not exists

    Returns:
        Dict: Item/Element information.
    """
    allowed_commands = {
        "thing": "id",
        "family": "id",
        "forumlist": "id",
        "forum": "id",
        "thread": "id",
        "user": "name",
        "guild": "id",
        "plays": "username",
        "collection": "username",
        "hot": "type",
        "search": "query",
    }
    if call_type not in allowed_commands.keys():
        raise Exception(
            "Api command does not exists. See https://boardgamegeek.com/wiki/page/BGG_XML_API2 for reference"
        )
    url = url_base + call_type
    parameters = {allowed_commands[call_type]: id} | extra_parameters
    response = requests.get(url=url, params=parameters)
    if verbose:
        logger.info(f"Getting information from API for {id}")

    try:
        return xmltodict.parse(response.content)["items"]["item"]
    except Exception as e:
        if verbose:
            logger.error(f"Failed getting information from API for {id}")
            logger.error(f"{e}")
        return {}


def get_from_name(name: str):
    boardgame_search = bgg_api_call(
        call_type="search",
        id=name,
        extra_parameters={"type": "boardgame"},
    )
    if isinstance(boardgame_search, list):
        boardgame_search = boardgame_search[0]
    id = int(boardgame_search["@id"])
    bg = get_from_id(id)

    return bg


def get_from_id(game_id: int):
    path_to_json = os.getenv("path_jsons")
    game_file = f"game_{str(game_id).zfill(6)}.json"
    game_path = os.path.join(path_to_json, game_file)
    try:
        json_files = [
            pos_json
            for pos_json in os.listdir(path_to_json)
            if pos_json.endswith(".json")
        ]
    except:
        json_files = []

    if game_file in json_files:
        with open(game_path) as json_file:
            boardgame_info = json.load(json_file)
    else:
        boardgame_info = bgg_api_call(call_type="thing", id=game_id)
        try:
            with open(game_path, "w") as f:
                json.dump(boardgame_info, f)
        except:
            pass
    bg = json_to_game(boardgame_info)
    return bg


def json_to_game(boardgame_info, replace_name: bool = True):
    bg = {}
    # ID
    bg["id"] = int(boardgame_info["@id"])
    # WEIGHT
    bg["weight"] = float(
        boardgame_info["statistics"]["ratings"]["averageweight"]["@value"]
    )
    # RATING
    bg["rating"] = float(boardgame_info["statistics"]["ratings"]["average"]["@value"])
    # YEAR
    bg["year"] = int(boardgame_info["yearpublished"]["@value"])
    # TYPE
    bg["type"] = boardgame_info["@type"]
    # MINPLAYERS
    bg["minplayers"] = int(boardgame_info["minplayers"]["@value"])
    bg["maxplayers"] = int(boardgame_info["maxplayers"]["@value"])
    bg["age"] = int(boardgame_info["minage"]["@value"])
    bg["minplaytime"] = int(boardgame_info["minplaytime"]["@value"])
    bg["maxplaytime"] = int(boardgame_info["maxplaytime"]["@value"])
    bg["rating_users"] = int(
        boardgame_info["statistics"]["ratings"]["usersrated"]["@value"]
    )
    bg["weight_users"] = int(
        boardgame_info["statistics"]["ratings"]["numweights"]["@value"]
    )
    bg["designer"] = {
        int(x["@id"]): normalize("NFKC", x["@value"])
        for x in boardgame_info["link"]
        if x["@type"] == "boardgamedesigner"
    }
    bg["mechanic"] = {
        int(x["@id"]): x["@value"].replace("'s", "s").replace("'", '"')
        for x in boardgame_info["link"]
        if x["@type"] == "boardgamemechanic"
    }
    if replace_name:
        # NAME
        names = boardgame_info["name"]
        if isinstance(names, list):
            bg["name"] = [x["@value"] for x in names if x["@type"] == "primary"][0]
        else:
            bg["name"] = names["@value"]
    return bg


def get_from_bgg(
    name: str = None,
    id: int = None,
):
    if id is not None:
        return get_from_id(id)
    elif name is not None:
        return get_from_name(name)
    else:
        print("Name and ID are empty. Please provide at least one of them")
