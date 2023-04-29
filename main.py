import sys
from modules.suggestions import suggest_games


def main():
    print(sys.argv)
    user = sys.argv[1]
    game_status = sys.argv[2] if len(sys.argv) > 2 else {"own": 1, "stats": 1}
    source = sys.argv[3] if len(sys.argv) > 3 else "rating"
    amount = sys.argv[4] if len(sys.argv) > 4 else 5
    top = sys.argv[5] if len(sys.argv) > 5 else 5

    suggest_games(user, game_status, source, amount, top)


if __name__ == "__main__":
    main()
