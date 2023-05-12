import os
import time
import logging
from modules.api_request import check_exists_db
from config.db_connection import run_query

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(getattr(logging, os.getenv("LOG_LEVEL", "INFO")))


class boardgame:
    def __init__(
        self,
        id: int = None,
        name: str = None,
        designer: str = None,
        mechanics: str = None,
        rating: float = None,
        year_published: int = None,
        type: str = None,
    ):
        self.id = int(id) if id else id
        self.name = str(name).replace('"', "'") if name else name
        self.designer = designer
        self.mechanics = mechanics
        self.rating = rating
        self.year_published = year_published
        self.type = str(type) if type else type

    def __str__(self):
        return f"""{self.type.title()}:
Name= {self.name}
ID= {self.id}
Designer= {self.designer}
Rating= {self.rating}
Published= {self.year_published}
Mechanics= {self.mechanics}"""

    def __repr__(self):
        return f"{self.name}({self.id})"

    def get_boardgame_information(self):
        bg_dict = check_exists_db(self.name, self.id)
        self.id = int(bg_dict["id"])
        self.name = str(bg_dict["name"]).replace('"', "'")
        self.designer = str(bg_dict["designer"])
        self.mechanics = str(bg_dict["mechanics"])
        self.rating = float(bg_dict["rating"])
        self.year_published = int(bg_dict["year_published"])
        self.type = str(bg_dict["type"])

    def save_to_db(self):
        query = f"""INSERT INTO boardgame(id,name,designer,mechanics,rating,year_published,type)
                    VALUES (?,?,?,?,?,?,?)
                    ON CONFLICT (id) DO NOTHING"""
        run_query(
            query,
            parameters=(
                self.id,
                self.name,
                self.designer,
                str(self.mechanics),
                self.rating,
                self.year_published,
                self.type,
            ),
            execute_only=True,
        )


def save_games(list_ids: list):
    list_games = [boardgame(id=ind_id) for ind_id in list_ids]
    list_games = [
        game for game in list_games if not check_exists_db(id=game.id, check_only=True)
    ]
    logger.info(
        f"From all {len(list_ids)} games {len(list_games)} are going to be inserted in the database"
    )
    for game in list_games:
        game.get_boardgame_information()
        game.save_to_db()
        logger.info(f"{game.name} inserted in database")
        time.sleep(2)
