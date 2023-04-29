from modules.db import db_init_, run_query
from modules.designers import get_designers, get_games_from_designer
from modules.config import database, designer_url

db_init_(database)
get_designers(designer_url)
query = f"""select distinct designer
            from designers"""
df_designers = run_query(query, execute_only=False)
count_designer = len(df_designers)
i = 0
for designer in df_designers.to_dict("records"):
    i += 1
    print("###################################################")
    print(
        f"Designer {str(i).zfill(2)}/{str(count_designer).zfill(2)}: Processing {designer['designer']}"
    )
    print("###################################################")
    get_games_from_designer(designer["designer"])
