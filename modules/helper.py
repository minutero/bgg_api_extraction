import os
import time
import logging
from modules.boardgame import boardgame
from modules.db import check_exists_db

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(getattr(logging, os.getenv("LOG_LEVEL", "INFO")))
