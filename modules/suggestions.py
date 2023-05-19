from modules.api_request import bgg_api_call
from modules.db import save_list_network_to_db
from config.db_connection import run_query
import pandas as pd
from pandas import DataFrame
import numpy as np
from config.config import columns
from dotenv_vault import load_dotenv

load_dotenv()


def suggest_games(user, **kwargs) -> DataFrame:
    results = 5 if not kwargs.get("results") else kwargs.get("results")
    top = 5 if not kwargs.get("top") else kwargs.get("top")
    game_status_all = (
        kwargs.get("game_status") | {"stats": 1}
        if kwargs.get("game_status")
        else {"stats": 1}
    )
    sort = "rating" if not kwargs.get("sort") else kwargs.get("sort")
    verbose = False if not kwargs.get("verbose") else kwargs.get("verbose")

    filter_db = {k: v for k, v in kwargs.items() if k in columns}
    user_collection = bgg_api_call("collection", user, game_status_all)
    games_id = [x["@objectid"] for x in user_collection if x["@subtype"] == "boardgame"]
    save_list_network_to_db(games_id, verbose)
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
        df_plays.sort_values([sort, "numplays"], ascending=False).head(top)["id"]
    )

    df_designer = get_designer_best(
        list_top, user_collection, results, True, 3, filter_db
    )
    df_mechanic = get_mechanics_best(
        list_top, user_collection, results, True, filter_db
    )
    df_suggestion = pd.concat([df_designer, df_mechanic])
    if verbose:
        print(df_suggestion)
    return df_suggestion


def get_designer_best(
    list_game_id,
    user_collection=None,
    results=5,
    no_expansion=True,
    top_by_designer=3,
    kwargs={},
):
    list_game_id_str = [str(x) for x in list_game_id]
    ids_to_drop = (
        ",".join(
            [k["@objectid"] for k in user_collection if int(k["status"]["@own"]) == 1]
        )
        if user_collection
        else "0"
    )
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
                and b.id not in ({ids_to_drop})
                and b.rating_users > 1000
                {" and " + " and ".join([k+" "+str(v) for k,v in kwargs.items()]) if kwargs else ""}
            order by bd.designer_id, b.rating DESC """,
    )
    df_designer_score = (
        df_designer_best[
            df_designer_best["rank"].isin(list(range(1, top_by_designer + 1)))
        ]
        .sort_values("rating", ascending=False)
        .drop_duplicates("name")
    )
    df_designer_score.loc[:, "name"] = (
        df_designer_score.name + " (" + df_designer_score.id.apply(str) + ")"
    )
    df_designer_top = df_designer_score.head(results)[
        ["name", "rating", "type"]
    ].reset_index(drop=True)
    return df_designer_top.assign(recommendation="designer")


def get_mechanics_best(
    list_game_id, user_collection=None, results=5, no_expansion=True, kwargs={}
):
    ids_to_drop = (
        ",".join(
            [k["@objectid"] for k in user_collection if int(k["status"]["@own"]) == 1]
        )
        if user_collection
        else "0"
    )
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
                and b.id not in ({ids_to_drop})
                and b.rating_users > 1000
                {" and " + " and ".join([k+" "+str(v) for k,v in kwargs.items()]) if kwargs else ""}
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
    ).drop_duplicates("id")
    df_mechanics_score.loc[:, "name"] = (
        df_mechanics_score.name + " (" + df_mechanics_score.id.apply(str) + ")"
    )
    df_mechanics_top = df_mechanics_score.head(results)[
        ["name", "rating", "type"]
    ].reset_index(drop=True)
    return df_mechanics_top.assign(recommendation="mechanics")
