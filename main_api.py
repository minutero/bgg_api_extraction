from fastapi import FastAPI
from modules.suggestions import suggest_games
from modules.likelihood_score import game_buy_score
from modules.api_request import get_from_name

app = FastAPI()


@app.get("/")
def index():
    return "Home"


@app.get("/suggest/{user}")
async def suggest_game(
    user: str,
    results: int = None,
    game_status: dict = None,
    sort: str = "rating",
    remove: str = None,
    where: str = None,
):
    if where:
        where = where.split(",")
    print(game_status)
    return suggest_games(
        user,
        results=results,
        game_status=game_status,
        sort=sort,
        remove=remove,
        where=where,
    )


@app.get("/like-score/{user}/{game}")
def like_score(user: str, game, weight: float = 1.0, sort: str = "rating"):
    if isinstance(game, int):
        game_id = game
    else:
        game_id = get_from_name(game)["id"]
    return game_buy_score(game_id, user, weight, sort)
