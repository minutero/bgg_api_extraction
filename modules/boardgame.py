import ast
from modules.api_request import get_from_id, get_from_name
from modules.db import check_exists_db


class boardgame:
    def __init__(
        self,
        name: str = None,
        id: int = None,
        designer: str = None,
        mechanics: list = None,
        rating: float = None,
        year_published: int = None,
    ):
        self.id = id
        self.name = name
        self.designer = designer
        self.mechanics = mechanics
        self.rating = rating
        self.year_published = year_published

    def __str__(self):
        return f"{self.name}({self.id})"

    def get_boardgame_information(self, replace_name: bool = True):
        exists, df_bg = check_exists_db(self.name, self.id)
        if not exists:
            if self.id is None and self.name is not None:
                bg = get_from_name(self.name, replace_name)
                self.id = bg.id
                self.designer = bg.designer
                self.mechanics = bg.mechanics
                self.rating = bg.rating
                self.year_published = bg.year_published
            elif self.name is None and self.id is not None:
                bg = get_from_id(self.id, replace_name)
                self.name = bg.name
                self.designer = bg.designer
                self.mechanics = bg.mechanics
                self.rating = bg.rating
                self.year_published = bg.year_published
        else:
            self.id = df_bg["id"]
            self.designer = df_bg["designer"]
            self.mechanics = ast.literal_eval(df_bg["mechanics"])
            self.rating = df_bg["rating"]
            self.year_published = df_bg["year_published"]
