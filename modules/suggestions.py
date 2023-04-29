from modules.api_request import bgg_api_call
from modules.db import run_query
from modules.designers import get_games_from_designer
from modules.boardgame import save_games
import pandas as pd
import numpy as np
import ast


def suggest_games(
    user, game_status={"own": 1, "stats": 1}, source="rating", results=5, top=5
):
    user_collection = bgg_api_call("collection", user, game_status)
    games_from_user_network(user_collection)
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
        f"""select id,
                        name,
                        designer,
                        rating,
                        type,
                        ROW_NUMBER() OVER (
                            PARTITION BY designer
                            ORDER BY rating DESC
                        ) as rank
                from boardgame b
                where {"type = 'boardgame' and " if no_expansion else ""}
                    designer in (select distinct designer from boardgame b2 where id in ({','.join(list_game_id_str)}))
                    and id not in ({','.join([k["@objectid"] for k in user_collection])})
                order by designer,rating DESC """,
        execute_only=False,
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
    df_mechanics = run_query(
        f"select distinct mechanics from boardgame b where id in ({','.join(list_game_id_str)})",
        execute_only=False,
    )
    list_mechanics = list(df_mechanics.mechanics.apply(ast.literal_eval))
    all_mechanics = [str(item) for sublist in list_mechanics for item in sublist]
    list_unique_mechanics = [*set(all_mechanics)]
    dict_weight_mechanics = {x: all_mechanics.count(x) for x in list_unique_mechanics}
    df_weight_mechanics = pd.DataFrame(
        dict_weight_mechanics.items(), columns=["mechanic", "count"]
    )
    df_mechanics_best = run_query(
        f"""select id,
                name,
                rating,
                type,
                mechanics
            from boardgame b
            where {"type = 'boardgame' and " if no_expansion else ""}
                ({' or '.join([f"mechanics like '%{x}%'" for x in list_unique_mechanics])})
                and id not in ({','.join([k["@objectid"] for k in user_collection])})""",
        execute_only=False,
    )
    lit_eval_if = lambda x: ast.literal_eval(x) if isinstance(x, str) else x
    df_mechanics_best.loc[:, "mechanic"] = df_mechanics_best["mechanics"].apply(
        lit_eval_if
    )

    df_mechanics_count = (
        df_mechanics_best.explode("mechanic")
        .merge(df_weight_mechanics, on="mechanic")
        .drop(columns=["mechanic"])
    )
    df_mechanics_score = (
        df_mechanics_count.groupby(
            [x for x in list(df_mechanics_count.columns) if x != "count"],
            as_index=False,
        )
        .agg("sum")
        .sort_values("count", ascending=False)
    )

    df_mechanics_top = df_mechanics_score.head(results)[
        ["name", "rating", "type"]
    ].reset_index(drop=True)
    return df_mechanics_top.assign(recommendation="mechanics")


def games_from_user_network(user_collection):
    games = [x["@objectid"] for x in user_collection if x["@subtype"] == "boardgame"]
    save_games(games)

    query = f"""select distinct designer
                from boardgame
                where id in ({','.join(games)})"""
    df_designers = run_query(query, execute_only=False)

    df_designer_game_count = run_query(
        "select designer,count(*) as games_count from boardgame b group by 1",
        execute_only=False,
    )
    df_designer_download = df_designers.merge(
        df_designer_game_count, how="inner", on="designer"
    )
    df_designer_download = df_designer_download[df_designer_download.games_count == 1]
    count_designer = len(df_designer_download)
    print("###################################################")
    print("Processing", count_designer, "designers")
    print("###################################################")
    i = 0
    for designer in df_designer_download.to_dict("records"):
        i += 1
        print("###################################################")
        print(
            f"Designer {str(i).zfill(2)}/{str(count_designer).zfill(2)}: Processing {designer['designer']}"
        )
        print("###################################################")
        get_games_from_designer(designer["designer"])
