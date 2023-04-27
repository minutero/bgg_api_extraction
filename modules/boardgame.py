from modules.config import database
from modules.db import check_exists_db, run_query


class boardgame:
    def __init__(
        self,
        id: int = None,
        name: str = None,
        designer: str = None,
        mechanics: list = None,
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
        self.mechanics = list(bg_dict["mechanics"])
        self.rating = float(bg_dict["rating"])
        self.year_published = int(bg_dict["year_published"])
        self.type = str(bg_dict["type"])

    def save_to_db(self, db_file=database):
        query = f"""INSERT OR IGNORE INTO boardgame(id,name,designer,mechanics,rating,year_published,type)
                    VALUES {self.id,
                            self.name.replace('"',"'"),
                            self.designer,
                            str(self.mechanics),
                            self.rating,
                            self.year_published,
                            self.type}"""
        run_query(
            query,
            db_file=db_file,
        )
