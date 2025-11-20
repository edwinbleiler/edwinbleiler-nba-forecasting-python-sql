"""
ingest_boxscores.py

Phase 3: Ingest real NBA game data & boxscores into SQLite.
Uses nba_api endpoints to fetch:
    - Daily scoreboard (game list)
    - BoxscoreTraditionalV2 (player-level stats)

Tables populated:
    - games
    - boxscores
"""

import time
from datetime import datetime
from pathlib import Path

import pandas as pd
from nba_api.stats.endpoints import ScoreboardV2, BoxScoreTraditionalV2
from nba_api.stats.library.http import NBAStatsHTTP

NBAStatsHTTP.headers.update({
    "Host": "stats.nba.com",
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/119.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.nba.com/",
    "Origin": "https://www.nba.com",
    "Connection": "keep-alive",
    "x-nba-stats-origin": "stats",
    "x-nba-stats-token": "true",
})

from db import get_connection, init_db


BASE_DIR = Path(__file__).resolve().parents[1]

# ---------------------------------------------------------
# Helpers
# ---------------------------------------------------------

def fetch_games_for_date(date_str: str) -> pd.DataFrame:
    """
    Fetch all games for a given date (YYYY-MM-DD).
    """
    scoreboard = ScoreboardV2(game_date=date_str.replace("-", ""))
    games = scoreboard.get_data_frames()[0]  # GameHeader table

    games = games[[
        "GAME_ID", "SEASON", "GAME_DATE_EST",
        "HOME_TEAM_ID", "VISITOR_TEAM_ID"
    ]].copy()

    games.rename(columns={
        "GAME_ID": "game_id",
        "SEASON": "season",
        "GAME_DATE_EST": "game_date",
        "HOME_TEAM_ID": "home_team_id",
        "VISITOR_TEAM_ID": "away_team_id"
    }, inplace=True)

    return games


def fetch_boxscores(game_id: str) -> pd.DataFrame:
    """
    Fetch boxscores for a single game using the BoxScoreTraditionalV2 endpoint.
    """
    box = BoxScoreTraditionalV2(game_id=game_id)
    df = box.get_data_frames()[0]  # PlayerStats table

    df = df[[
        "GAME_ID", "PLAYER_ID", "TEAM_ID", "OPPONENT_TEAM_ID",
        "MIN", "PTS", "REB", "AST", "STL", "BLK", "TO"
    ]].copy()

    df.rename(columns={
        "GAME_ID": "game_id",
        "PLAYER_ID": "player_id",
        "TEAM_ID": "team_id",
        "OPPONENT_TEAM_ID": "opponent_team_id",
        "MIN": "minutes",
        "PTS": "points",
        "REB": "rebounds",
        "AST": "assists",
        "STL": "steals",
        "BLK": "blocks",
        "TO": "turnovers"
    }, inplace=True)

    return df


# ---------------------------------------------------------
# Database writes
# ---------------------------------------------------------

def upsert_games(df_games: pd.DataFrame):
    with get_connection() as conn:
        cursor = conn.cursor()
        sql = """
        INSERT OR REPLACE INTO games (
            game_id, season, game_date,
            home_team_id, away_team_id
        ) VALUES (
            :game_id, :season, :game_date,
            :home_team_id, :away_team_id
        );
        """
        cursor.executemany(sql, df_games.to_dict("records"))
        conn.commit()


def insert_boxscores(df_box: pd.DataFrame):
    with get_connection() as conn:
        cursor = conn.cursor()
        sql = """
        INSERT INTO boxscores (
            game_id, player_id, team_id, opponent_team_id,
            minutes, points, rebounds, assists,
            steals, blocks, turnovers
        ) VALUES (
            :game_id, :player_id, :team_id, :opponent_team_id,
            :minutes, :points, :rebounds, :assists,
            :steals, :blocks, :turnovers
        );
        """
        cursor.executemany(sql, df_box.to_dict("records"))
        conn.commit()


# ---------------------------------------------------------
# Main ingestion function
# ---------------------------------------------------------

def ingest_date(date_str: str):
    """Ingest all games and boxscores for a given date."""
    print(f"Fetching games for {date_str}...")
    games = fetch_games_for_date(date_str)

    if games.empty:
        print("No games found.")
        return

    print(f"Found {len(games)} games. Inserting...")
    upsert_games(games)

    print("Fetching boxscores...")
    for game_id in games["game_id"]:
        print(f"  - {game_id}")
        box = fetch_boxscores(game_id)
        insert_boxscores(box)
        time.sleep(0.8)  # API-friendly delay

    print("Ingestion complete for:", date_str)


if __name__ == "__main__":
    import sys
    from datetime import timedelta

    init_db()

    # If user passes a date argument: use that
    if len(sys.argv) > 1:
        date_str = sys.argv[1]
    else:
        # Default: yesterday (to avoid empty future dates)
        date_str = (datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")

    print(f"Running ingestion for date: {date_str}")
    ingest_date(date_str)

