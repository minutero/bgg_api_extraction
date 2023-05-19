import os
import argparse
import logging
from modules.suggestions import suggest_games
from dotenv_vault import load_dotenv

load_dotenv()
logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(getattr(logging, os.getenv("LOG_LEVEL", "INFO")))


def main_args():
    # Initialize parser
    msg = "Adding description"
    parser = argparse.ArgumentParser(description=msg)
    parser.add_argument("-u", "--User", help="User from BGG to give recommendation")
    parser.add_argument(
        "-r",
        "--Results",
        default=5,
        type=int,
        help="Amount of games printed by recommendation's category",
    )
    parser.add_argument(
        "-t",
        "--Top",
        default=5,
        type=int,
        help="Amount of games from user's collection to be used as the source of recommendation",
    )
    parser.add_argument(
        "-s",
        "--Sort",
        default="rating",
        help="Charateristic to be used for sorting the best games in the user's collection. Default 'Rating'",
    )
    parser.add_argument(
        "-g",
        "--GameStatus",
        help="Game's status to be used as a filter when getting the user's collection",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        help="Increase output verbosity",
        action="store_true",
    )

    return parser.parse_args()


def main():
    args = main_args()
    if args.verbose:
        logger.info("Verbosity turned on")
    suggest_games(
        args.User,
        game_status=args.GameStatus,
        sort=args.Sort,
        results=args.Results,
        top=args.Top,
        verbose=args.verbose,
    )


if __name__ == "__main__":
    main()
