import os
import time
import logging
from modules.api_request import check_exists_db
from config.db_connection import run_query
from config.config import columns

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(getattr(logging, os.getenv("LOG_LEVEL", "INFO")))


class boardgame:
    def __init__(
        self,
        id: int = None,
        name: str = None,
        weight: float = None,
        rating: float = None,
        year: int = None,
        type: str = None,
        minplayers: int = None,
        maxplayers: int = None,
        age: int = None,
        minplaytime: int = None,
        maxplaytime: int = None,
        rating_users: int = None,
        weight_users: int = None,
    ):
        self.id = int(id) if id else id
        self.name = str(name).replace('"', "'") if name else name
        self.weight = float(weight) if weight else weight
        self.rating = float(rating) if rating else rating
        self.year = int(year) if year else year
        self.type = str(type) if type else type
        self.minplayers = int(minplayers) if minplayers else minplayers
        self.maxplayers = int(maxplayers) if maxplayers else maxplayers
        self.age = int(age) if age else age
        self.minplaytime = int(minplaytime) if minplaytime else minplaytime
        self.maxplaytime = int(maxplaytime) if maxplaytime else maxplaytime
        self.rating_users = int(rating_users) if rating_users else rating_users
        self.weight_users = int(weight_users) if weight_users else weight_users

    def __str__(self):
        return f"""{self.type.title()}:
ID= {self.id}
Name= {self.name}
Rating= {self.rating}
Weight= {self.weight}
Published= {self.year}"""

    def __repr__(self):
        return f"{self.name}({self.id})"

    def get_boardgame_information(self):
        bg_dict = check_exists_db(self.name, self.id)
        self.id = int(bg_dict["id"])
        self.name = str(bg_dict["name"]).replace('"', "'")
        self.weight = float(bg_dict["weight"])
        self.rating = float(bg_dict["rating"])
        self.year = int(bg_dict["year"])
        self.type = str(bg_dict["type"])
        self.minplayers = int(bg_dict["minplayers"])
        self.maxplayers = int(bg_dict["maxplayers"])
        self.age = int(bg_dict["age"])
        self.minplaytime = int(bg_dict["minplaytime"])
        self.maxplaytime = int(bg_dict["maxplaytime"])
        self.rating_users = int(bg_dict["rating_users"])
        self.weight_users = int(bg_dict["weight_users"])

    def save_to_db(self):
        query = f"""INSERT INTO boardgames.boardgame({",".join(columns)})
                    VALUES ({(",%s"*len(columns))[1:]})
                    ON CONFLICT (id) DO NOTHING"""
        run_query(
            query,
            parameters=(
                self.id,
                self.name,
                self.weight,
                self.rating,
                self.year,
                self.type,
                self.minplayers,
                self.maxplayers,
                self.age,
                self.minplaytime,
                self.maxplaytime,
                self.rating_users,
                self.weight_users,
            ),
            execute_only=True,
        )


def save_games(list_ids: list):
    already_db = (
        run_query(
            f"select id from boardgames.boardgame where id in ({','.join(list_ids)})"
        )
        .id.apply(str)
        .to_list()
    )
    list_games = [
        boardgame(id=ind_id) for ind_id in list_ids if ind_id not in already_db
    ]
    logger.info(
        f"From all {len(list_ids)} games {len(list_games)} are going to be inserted in the database"
    )
    for game in list_games:
        game.get_boardgame_information()
        game.save_to_db()
        logger.info(f"{game.name} inserted in database")
        time.sleep(2)
