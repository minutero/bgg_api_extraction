import requests
import xmltodict
import pandas as pd


# url = "https://boardgamegeek.com/xmlapi/boardgame/342942?stats=1"
url_base = "https://boardgamegeek.com/xmlapi2/"
apis = [
    "thing",
    "family",
    "forumlist",
    "forum",
    "thread",
    "user",
    "guild",
    "plays",
    "collection",
    "hot",
    "search",
]
url = url_base + apis[0]
parameters = {"id": 167791, "type": "boardgame", "stats": 1, "pagesize": 100}
response = requests.get(url=url, params=parameters)

dict_response = xmltodict.parse(response.content)["items"]["item"]

designer = [
    x["@value"] for x in dict_response["link"] if x["@type"] == "boardgamedesigner"
][0]
rating = dict_response["statistics"]["ratings"]["average"]["@value"]
