import os
import logging
from modules.api_request import get_from_bgg, json_to_game
from config.db_connection import run_query
from config.config import columns
from typing import Mapping

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
        designer: Mapping = {},
        mechanic: Mapping = {},
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
        self.designer = designer
        self.mechanic = mechanic

    def __str__(self):
        return f"""{self.type.title()}:
ID = {self.id}
Name = {self.name}
Rating = {self.rating}
Weight = {self.weight}
Published = {self.year}"""

    def __repr__(self):
        return f"{self.name}({self.id})"

    def get_boardgame_information(self):
        bg_dict = get_from_bgg(self.name, self.id)
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
        self.designer = bg_dict["designer"]
        self.mechanic = bg_dict["mechanic"]

    def save_to_db(self, update=True):
        query = f"""INSERT INTO boardgames.boardgame({",".join(columns)})
                    VALUES ({(",%s"*len(columns))[1:]})
                    ON CONFLICT (id) DO {"UPDATE set name = '" + self.name + "'" +
                                        ", weight = " + str(self.weight) +
                                        ", rating = " + str(self.rating) +
                                        ", rating_users = " + str(self.rating_users) +
                                        ", weight_users = " + str(self.weight_users) +
                                        " where boardgame.id = " + str(self.id)
                                        if update else "NOTHING"}"""
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
        for designer_id, designer_name in self.designer.items():
            sql = """INSERT INTO boardgames.bg_x_designer (game_id, designer_id)
                    VALUES (%s, %s)
                    ON CONFLICT (game_id, designer_id) DO NOTHING
                """
            run_query(sql, execute_only=True, parameters=(self.id, designer_id))
            sql = """INSERT INTO boardgames.designer (id, name)
                    VALUES (%s, %s)
                    ON CONFLICT (id) DO NOTHING
                """
            run_query(sql, execute_only=True, parameters=(designer_id, designer_name))

        for mechanic_id, mechanic_name in self.mechanic.items():
            sql = """INSERT INTO boardgames.bg_x_mechanic (game_id, mechanic_id)
                    VALUES (%s, %s)
                    ON CONFLICT (game_id, mechanic_id) DO NOTHING
                """
            run_query(sql, execute_only=True, parameters=(self.id, mechanic_id))
            sql = """INSERT INTO boardgames.mechanics (id, name)
                    VALUES (%s, %s)
                    ON CONFLICT (id) DO NOTHING
                """
            run_query(sql, execute_only=True, parameters=(mechanic_id, mechanic_name))

    def bg_to_list(self, items=["game", "designer", "mechanic"]):
        dict_result = {}
        if "game" in items:
            game_list = [
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
            ]
            dict_result["game"] = game_list
        if "designer" in items:
            dict_result["designer"] = [[k, v] for k, v in self.designer.items()]
        if "mechanic" in items:
            dict_result["mechanic"] = [[k, v] for k, v in self.mechanic.items()]

        return dict_result
