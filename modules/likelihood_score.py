from modules.api_request import bgg_api_call
from modules.db import run_query
from modules.boardgame import boardgame
import pandas as pd
import numpy as np


def game_buy_score(
    game_id: int,
    user: str,
    sort: str = "rating",
    weight: float = 0.9,
    verbose=False,
    **kwargs,
) -> float:
    game_status_all = (
        kwargs.get("game_status") | {"stats": 1}
        if kwargs.get("game_status")
        else {"stats": 1, "own": 1}
    )
    bg = boardgame(id=game_id)
    bg.get_boardgame_information()

    user_collection = bgg_api_call("collection", user, game_status_all)
    games_id = [
        x["@objectid"]
        for x in user_collection
        if x["@subtype"] == "boardgame" and x["@objectid"] != str(bg.id)
    ]

    df_plays = (
        pd.DataFrame(
            [
                [k["@objectid"], k["numplays"], k["stats"]["rating"]["@value"]]
                for k in user_collection
            ],
            columns=["game_id", "numplays", "rating"],
        )
        .replace("N/A", np.nan)
        .astype({"game_id": int, "numplays": float, "rating": float})
        .fillna({"rating": 0})
    ).sort_values([sort, "numplays"], ascending=False)
    df_boardgame = run_query(
        f"""select bm.game_id
                    ,m."name" as mechanic
                    ,d."name" as designer
                from boardgames.bg_x_mechanic bm
                    inner join boardgames.mechanics m on bm.mechanic_id = m.id
                    inner join boardgames.bg_x_designer bd on bm.game_id = bd.game_id
                    inner join boardgames.designer d on bd.designer_id = d.id
                where bm.game_id in ({",".join(games_id)})"""
    )
    df_all = df_boardgame.merge(df_plays, on="game_id")
    max_plays = df_all.numplays.max()
    wr = weight if sort == "rating" else 1 - weight
    wn = 1.0 - wr
    df_all.loc[:, "score"] = (
        df_all.numplays / max_plays
    ) * 10 * wn + df_all.rating * wr

    mechanic_score = (
        df_all[["mechanic", "score"]]
        .drop_duplicates()
        .groupby("mechanic")
        .sum()
        .sort_values("score", ascending=False)
    )
    designer_score = (
        df_all[["designer", "score"]]
        .drop_duplicates()
        .groupby("designer")
        .sum()
        .sort_values("score", ascending=False)
    )
    game_mechanic_score = mechanic_score[
        mechanic_score.index.isin(bg.mechanic.values())
    ].sum()[0]
    game_designer_score = designer_score[
        designer_score.index.isin(bg.designer.values())
    ].sum()[0]
    score = game_designer_score * wn + game_mechanic_score * wr
    if verbose:
        print(
            f"""Buying Score for {bg.name} is {score}.
                {round(game_designer_score,2)}*{round(wn,2) + round(game_mechanic_score,2)}*{round(wr,2)}"""
        )
    else:
        return score
