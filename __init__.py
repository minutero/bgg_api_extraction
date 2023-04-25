from modules.db import db_init_
from modules.designers import get_designers
from modules.config import database, designer_url

db_init_(database)
get_designers(designer_url)
