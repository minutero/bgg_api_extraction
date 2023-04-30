import sys
from modules.suggestions import suggest_games


def main():
    user = sys.argv[1]
    results = int(sys.argv[2]) if len(sys.argv) > 2 else 5
    top = int(sys.argv[3]) if len(sys.argv) > 3 else 5
    game_status = sys.argv[4] if len(sys.argv) > 4 else {"own": 1, "stats": 1}
    source = sys.argv[5] if len(sys.argv) > 5 else "rating"

    suggest_games(user, game_status, source, results, top)


if __name__ == "__main__":
    main()
