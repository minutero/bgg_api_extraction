import re
from modules.api_request import bgg_api_call
from modules.db import save_list_network_to_db
from config.db_connection import run_query
import pandas as pd
from pandas import DataFrame
import numpy as np
from config.config import columns
from thefuzz import fuzz
from thefuzz import process
from dotenv_vault import load_dotenv

load_dotenv()


def suggest_games(user, **kwargs) -> DataFrame:
    # Console parameters to variables
    results = 5 if not kwargs.get("results") else kwargs.get("results")
    game_status_all = (
        kwargs.get("game_status") | {"stats": 1}
        if kwargs.get("game_status")
        else {"own": 1, "stats": 1}
    )
    sort = "rating" if not kwargs.get("sort") else kwargs.get("sort")
    remove = "" if not kwargs.get("remove") else kwargs.get("remove")
    where = [] if not kwargs.get("where") else kwargs.get("where")
    verbose = False if not kwargs.get("verbose") else kwargs.get("verbose")

    remove_ids = {"id": remove.split(",")} if remove else {}
    # Transform where parameter to SQL syntax
    rep = {"gt": ">", "ge": ">=", "lt": "<", "le": "<=", "eq": "=", "ne": "<>"}
    pattern = re.compile("|".join(rep.keys()))
    f = lambda m: rep[re.escape(m.group(0))]
    where_sql_symbol = [pattern.sub(f, c).replace("+", " ") for c in where]
    where_clause = {
        k.split(" ")[0]: " ".join(k.split(" ")[1:])
        for k in where_sql_symbol
        if k.split(" ")[0] in columns
    } | remove_ids
    user_collection = bgg_api_call("collection", user, game_status_all)
    games_id = [x["@objectid"] for x in user_collection if x["@subtype"] == "boardgame"]
    save_list_network_to_db(games_id, verbose)
    bg_expansion = run_query(
        f"""select id
            from boardgames.boardgame
            where type = 'boardgameexpansion'
                and id in ({",".join([str(k["@objectid"]) for k in user_collection])})"""
    ).id.to_list()

    df_plays = (
        pd.DataFrame(
            [
                [k["@objectid"], k["numplays"], k["stats"]["rating"]["@value"]]
                for k in user_collection
                if int(k["@objectid"]) not in bg_expansion
            ],
            columns=["id", "numplays", "rating"],
        )
        .replace("N/A", np.nan)
        .astype({"id": int, "numplays": int, "rating": float})
    )
    if not kwargs.get("top"):
        tot = len(df_plays)
        if (
            df_plays.isna().sum()["numplays"] == tot
            and df_plays.isna().sum()["rating"] == tot
        ):
            top = tot
        else:
            top = 10
    else:
        top = kwargs.get("top")
    list_rank = df_plays.sort_values([sort, "numplays"], ascending=False)["id"]
    own_games = (
        {
            str(k["@objectid"]): k["name"]["#text"]
            for k in user_collection
            if int(k["status"]["@own"]) == 1
        }
        if user_collection
        else "0"
    )
    df_designer = get_designer_best(
        list(list_rank.head(top)), own_games, results, True, 3, True, where_clause
    )
    df_mechanic = get_mechanics_best(
        list(list_rank.head(top)), own_games, results, True, True, where_clause
    )
    df_suggestion = pd.concat([df_designer, df_mechanic]).reset_index(drop=True)
    if verbose:
        print(df_suggestion)
    return df_suggestion.to_dict("records")


def get_designer_best(
    list_game_id,
    own_games=None,
    results=5,
    no_expansion=True,
    top_by_designer=3,
    remove_similar=True,
    kwargs={},
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
                and b.id not in ({",".join(own_games.keys())})
                and b.rating_users > 1000
                {" and " + (
                    " and ".join([k + " " + v if not isinstance(v,list)
                                    else k + " not in (" + ", ".join(v) + ")"
                                    for k, v in kwargs.items()
                                ])) if kwargs else ""}
            order by bd.designer_id, b.rating DESC """,
    )
    if remove_similar:
        df_designer_best = remove_similar_games(
            df_designer_best, list(own_games.values())
        )

    df_designer_score = (
        df_designer_best[
            df_designer_best["rank"].isin(list(range(1, top_by_designer + 1)))
        ]
        .sort_values("rating", ascending=False)
        .drop_duplicates("name")
    )
    df_designer_top = df_designer_score.head(results)[
        ["id", "name", "rating", "type"]
    ].reset_index(drop=True)
    return df_designer_top.assign(recommendation="designer")


def get_mechanics_best(
    list_game_id,
    own_games=None,
    results=5,
    no_expansion=True,
    remove_similar=True,
    kwargs={},
):
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
                and b.id not in ({",".join(own_games.keys())})
                and b.rating_users > 1000
                {" and " + (
                    " and ".join([k + " " + v if not isinstance(v,list)
                                    else k + " not in (" + ", ".join(v) + ")"
                                    for k, v in kwargs.items()
                                ])) if kwargs else ""}
            order by bd.mechanic_id, b.rating DESC """
    )
    if remove_similar:
        df_mechanics_best = remove_similar_games(
            df_mechanics_best, list(own_games.values())
        )

    df_mechanics_count = df_mechanics_best.merge(
        df_weight_mechanics, on="mechanic_id"
    ).drop(columns=["mechanic_id"])
    df_mechanics_score = (
        df_mechanics_count.groupby(
            [x for x in list(df_mechanics_count.columns) if x not in ["count", "rank"]],
            as_index=False,
        )
        .agg("sum")
        .sort_values(["count", "rating"], ascending=False)
    )
    df_mechanics_top = df_mechanics_score.head(results)[
        ["id", "name", "rating", "type"]
    ].reset_index(drop=True)
    return df_mechanics_top.assign(recommendation="mechanics")


def remove_similar_games(df, remove):
    def similar(query):
        return process.extractOne(query, remove, scorer=fuzz.token_sort_ratio)[1]

    df.loc[:, "similarity"] = df.name.apply(similar)
    return df[df.similarity <= 60]
