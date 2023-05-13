from modules.api_request import bgg_api_call
from modules.db import save_list_network_to_db
from config.db_connection import run_query
import pandas as pd
import numpy as np


def suggest_games(user, game_status={"stats": 1}, source="rating", results=5, top=5):
    user_collection = bgg_api_call("collection", user, game_status)
    games_id = [x["@objectid"] for x in user_collection if x["@subtype"] == "boardgame"]
    save_list_network_to_db(games_id)
    df_plays = (
        pd.DataFrame(
            [
                [k["@objectid"], k["numplays"], k["stats"]["rating"]["@value"]]
                for k in user_collection
            ],
            columns=["id", "numplays", "rating"],
        )
        .replace("N/A", np.nan)
        .astype({"id": int, "numplays": int, "rating": float})
    )

    list_top = list(
        df_plays.sort_values([source, "numplays"], ascending=False).head(top)["id"]
    )

    df_designer = get_designer_best(
        list_top, user_collection, results=results, top_by_designer=3
    )
    df_mechanic = get_mechanics_best(list_top, user_collection, results=results)
    df_suggestion = pd.concat([df_designer, df_mechanic])
    print(df_suggestion)
    return df_suggestion


def get_designer_best(
    list_game_id, user_collection, no_expansion=True, top_by_designer=1, results=5
):
    list_game_id_str = [str(x) for x in list_game_id]
    df_designer_best = run_query(
        f"""select b.id,
                    b.name,
                    bd.designer_id,
                    b.rating,
                    b.type,
                    ROW_NUMBER() OVER (
                        PARTITION BY bd.designer_id
                        ORDER BY b.rating DESC
                    ) as rank
            from boardgames.boardgame b
            inner join boardgames.bg_x_designer bd on b.id = bd.game_id
            where {"type = 'boardgame' and " if no_expansion else ""}
                bd.designer_id in (select distinct designer_id
                                    from boardgames.bg_x_designer
                                    where game_id in ({','.join(list_game_id_str)}))
                and b.id not in ({','.join([k["@objectid"] for k in user_collection if int(k["status"]["@own"])==1])})
                and b.rating_users > 1000
            order by bd.designer_id, b.rating DESC """,
    )
    df_designer_score = df_designer_best[
        df_designer_best["rank"].isin(list(range(1, top_by_designer + 1)))
    ].sort_values("rating", ascending=False)
    df_designer_top = df_designer_score.head(results)[
        ["name", "rating", "type"]
    ].reset_index(drop=True)
    return df_designer_top.assign(recommendation="designer")


def get_mechanics_best(list_game_id, user_collection, no_expansion=True, results=5):
    list_game_id_str = [str(x) for x in list_game_id]
    df_weight_mechanics = run_query(
        f"""select mechanic_id, count(game_id) as "count"
            from boardgames.bg_x_mechanic
            where game_id in ({','.join(list_game_id_str)})
            group by 1""",
    )

    df_mechanics_best = run_query(
        f"""select b.id,
                    b.name,
                    bd.mechanic_id,
                    b.rating,
                    b.type,
                    ROW_NUMBER() OVER (
                        PARTITION BY bd.mechanic_id
                        ORDER BY b.rating DESC
                    ) as rank
            from boardgames.boardgame b
            inner join boardgames.bg_x_mechanic bd on b.id = bd.game_id
            where {"type = 'boardgame' and " if no_expansion else ""}
                bd.mechanic_id in (select distinct mechanic_id
                                    from boardgames.bg_x_mechanic
                                    where game_id in ({','.join(list_game_id_str)}))
                and b.id not in ({','.join([k["@objectid"] for k in user_collection if int(k["status"]["@own"])==1])})
                and b.rating_users > 1000
            order by bd.mechanic_id, b.rating DESC """
    )

    df_mechanics_count = df_mechanics_best.merge(
        df_weight_mechanics, on="mechanic_id"
    ).drop(columns=["mechanic_id"])
    df_mechanics_score = (
        df_mechanics_count.groupby(
            [x for x in list(df_mechanics_count.columns) if x != "count"],
            as_index=False,
        )
        .agg("sum")
        .sort_values(["count", "rating"], ascending=False)
    )

    df_mechanics_top = df_mechanics_score.head(results)[
        ["name", "rating", "type"]
    ].reset_index(drop=True)
    return df_mechanics_top.assign(recommendation="mechanics")
