import os
import requests
import xmltodict
import logging
import pandas as pd
from typing import Dict, Mapping
from unicodedata import normalize
from config.config import url_base
from config.db_connection import run_query

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

    return xmltodict.parse(response.content)["items"]["item"]


def get_from_name(name: str, replace_name: bool = True):
    boardgame_search = bgg_api_call(
        call_type="search",
        id=name,
        extra_parameters={"exact": 1, "type": "boardgame"},
    )
    id = int(boardgame_search["@id"])
    bg = get_from_id(id, replace_name)

    return bg


def get_from_id(id: int, replace_name: bool = True):
    bg = {"id": id}
    boardgame_info = bgg_api_call(call_type="thing", id=bg["id"])
    designer = [
        normalize("NFKC", x["@value"])
        for x in boardgame_info["link"]
        if x["@type"] == "boardgamedesigner"
    ]
    bg["designer"] = designer[0] if len(designer) > 0 else None
    bg["mechanics"] = (
        str(
            [
                x["@value"]
                for x in boardgame_info["link"]
                if x["@type"] == "boardgamemechanic"
            ]
        )
        .replace("'s", "s")
        .replace("'", '"')
    )
    bg["rating"] = float(boardgame_info["statistics"]["ratings"]["average"]["@value"])
    bg["year_published"] = int(boardgame_info["yearpublished"]["@value"])
    bg["type"] = str(boardgame_info["@type"])
    if replace_name:
        names = boardgame_info["name"]
        if isinstance(names, list):
            bg["name"] = [x["@value"] for x in names if x["@type"] == "primary"][0]
        else:
            bg["name"] = names["@value"]
    return bg


def check_exists_db(
    name: str = None,
    id: int = None,
    replace_name: bool = True,
    check_only: bool = False,
):
    bg = run_query(
        f"SELECT * FROM boardgame where name = '{name}' or id = {id if id else 0}",
    )

    if bg.empty:
        if check_only:
            return False
        if id is not None:
            bg = pd.json_normalize(get_from_id(id, replace_name))
        elif name is not None:
            bg = pd.json_normalize(get_from_name(name, replace_name))
        else:
            print("Name and ID are empty. Please provide at least one of them")
    else:
        if check_only:
            return True
    return bg.to_dict(orient="records")[0]
