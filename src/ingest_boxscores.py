"""
ingest_boxscores.py

Phase 3: Ingest real NBA game data & boxscores into SQLite.
Now uses BoxScoreTraditionalV3 (current endpoint).
"""

import time
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from nba_api.stats.endpoints import ScoreboardV2, BoxScoreTraditionalV3
from nba_api.stats.library.http import NBAStatsHTTP

from db import get_connection, init_db


# ---------------------------------------------------------------------
# Proper headers (works in 2025)
# ---------------------------------------------------------------------
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
})

# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------

def fetch_games_for_date(date_str: str) -> pd.DataFrame:
    """Fetch all games scheduled for a given date."""
    scoreboard = ScoreboardV2(game_date=date_str.replace("-", ""))
    df = scoreboard.get_data_frames()[0]  # GameHeader

    df = df[[
        "GAME_ID", "SEASON", "GAME_DATE_EST",
        "HOME_TEAM_ID", "VISITOR_TEAM_ID"
    ]].copy()

    df.rename(columns={
        "GAME_ID": "game_id",
        "SEASON": "season",
        "GAME_DATE_EST": "game_date",
        "HOME_TEAM_ID": "home_team_id",
        "VISITOR_TEAM_ID": "away_team_id",
    }, inplace=True)

    return df


def fetch_boxscores(game_id: str, home_team: int, away_team: int) -> pd.DataFrame:
    """Fetch boxscores for a single game using V3 endpoint."""
    box = BoxScoreTraditionalV3(game_id=game_id)
    df = box.get_data_frames()[0]

    # Compute opponent_team_id manually
    df["opponent_team_id"] = df["teamId"].apply(
        lambda tid: away_team if tid == home_team else home_team
    )

    # Select simplified columns for our schema
    df = df[[
        "gameId",
        "personId",
        "teamId",
        "opponent_team_id",
        "minutes",
        "points",
        "reboundsTotal",
        "assists",
        "steals",
        "blocks",
        "turnovers",
    ]].copy()

    df.rename(columns={
        "gameId": "game_id",
        "personId": "player_id",
        "teamId": "team_id",
        "reboundsTotal": "rebounds",
    }, inplace=True)

    return df


# ---------------------------------------------------------------------
# Database writes
# ---------------------------------------------------------------------

def upsert_games(df: pd.DataFrame):
    with get_connection() as conn:
        cur = conn.cursor()
        sql = """
        INSERT OR REPLACE INTO games (
            game_id, season, game_date,
            home_team_id, away_team_id
        ) VALUES (
            :game_id, :season, :game_date,
            :home_team_id, :away_team_id
        );
        """
        cur.executemany(sql, df.to_dict("records"))
        conn.commit()


def insert_boxscores(df: pd.DataFrame):
    with get_connection() as conn:
        cur = conn.cursor()
        sql = """
        INSERT INTO boxscores (
            game_id, player_id, team_id, opponent_team_id,
            minutes, points, rebounds, assists, steals, blocks, turnovers
        ) VALUES (
            :game_id, :player_id, :team_id, :opponent_team_id,
            :minutes, :points, :rebounds, :assists,
            :steals, :blocks, :turnovers
        );
        """
        cur.executemany(sql, df.to_dict("records"))
        conn.commit()


# ---------------------------------------------------------------------
# Full ingestion runner
# ---------------------------------------------------------------------

def ingest_date(date_str: str):
    print(f"Fetching games for {date_str}...")
    games = fetch_games_for_date(date_str)

    if games.empty:
        print("No games found.")
        return

    print(f"Found {len(games)} games. Inserting...")
    upsert_games(games)

    print("Fetching boxscores...")

    for _, row in games.iterrows():
        game_id = row["game_id"]
        home_team = row["home_team_id"]
        away_team = row["away_team_id"]

        print(f"  - {game_id}")

        try:
            df_box = fetch_boxscores(game_id, home_team, away_team)
            insert_boxscores(df_box)
        except Exception as e:
            print(f"Error fetching {game_id}: {e}")

        time.sleep(0.6)

    print("Ingestion complete for", date_str)


# ---------------------------------------------------------------------
# CLI runner
# ---------------------------------------------------------------------
if __name__ == "__main__":
    import sys

    init_db()

    if len(sys.argv) > 1:
        date_str = sys.argv[1]
    else:
        date_str = (datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")

    print(f"Running ingestion for {date_str}")
    ingest_date(date_str)
