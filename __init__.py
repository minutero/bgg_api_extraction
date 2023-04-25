from modules.db import db_init_
from modules.designers import get_designers

db = "boardgame.db"
url = "https://boardgamegeek.com/browse/boardgamedesigner"

db_init_(db)
get_designers(url)
