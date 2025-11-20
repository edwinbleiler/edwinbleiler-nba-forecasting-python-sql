"""
ingest_boxscores.py

Phase 3: Ingest real NBA game data & boxscores into SQLite.

Fixes:
- ScoreboardV2 (nba_api) is unreliable / blocked by NBA
- Replaced by direct scoreboardv3 API calls with browser headers

Tables populated:
    - games
    - boxscores
"""

import time
from datetime import datetime, timedelta
from pathlib import Path
import requests
import pandas as pd

# nba_api still reliable for boxscores
from nba_api.stats.endpoints import BoxScoreTraditionalV3
from nba_api.stats.library.http import NBAStatsHTTP

# Browser headers to bypass NBA Cloudflare protection
NBAStatsHTTP.headers.update({
    "Host": "stats.nba.com",
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.nba.com/",
    "Origin": "https://www.nba.com",
    "Connection": "keep-alive",
    "x-nba-stats-origin": "stats",
    "x-nba-stats-token": "true",
    "Sec-Fetch-Site": "same-origin",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Dest": "empty",
})

from db import get_connection, init_db

BASE_DIR = Path(__file__).resolve().parents[1]

# ---------------------------------------------------------------------
# Fetch games using scoreboardv3 (fully reliable)
# ---------------------------------------------------------------------
def fetch_games_for_date(date_str: str) -> pd.DataFrame:
    """
    Fetch games for a given YYYY-MM-DD date using NBA Stats API scoreboardv3.
    """

    url = "https://stats.nba.com/stats/scoreboardv3"

    headers = {
        "Host": "stats.nba.com",
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.nba.com/",
        "Origin": "https://www.nba.com",
        "Connection": "keep-alive",
        "x-nba-stats-origin": "stats",
        "x-nba-stats-token": "true",
        "Sec-Fetch-Site": "same-origin",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Dest": "empty",
    }

    params = {"GameDate": date_str, "LeagueID": "00", "DayOffset": "0"}

    response = requests.get(url, headers=headers, params=params, timeout=10)

    if response.status_code != 200:
        print(f"ERROR: HTTP {response.status_code}")
        print(response.text[:300])
        return pd.DataFrame()

    data = response.json()
    games_list = data.get("scoreboard", {}).get("games", [])

    if not games_list:
        print(f"No games found for {date_str}")
        return pd.DataFrame()

    rows = []
    for g in games_list:
        rows.append({
            "game_id": g.get("gameId"),
            "season": g.get("season"),
            "game_date": g.get("gameDateEst"),
            "home_team_id": g.get("homeTeam", {}).get("teamId"),
            "away_team_id": g.get("awayTeam", {}).get("teamId"),
        })

    return pd.DataFrame(rows)


# ---------------------------------------------------------------------
# Fetch boxscores using BoxScoreTraditionalV2
# ---------------------------------------------------------------------
def fetch_boxscores(game_id: str) -> pd.DataFrame:
    """
    Fetch player boxscore stats for a single game using BoxScoreTraditionalV3
    (2025+ correct schema).
    """

    box = BoxScoreTraditionalV3(game_id=game_id)
    df = box.get_data_frames()[0]

    # New schema columns in TraditionalV3
    expected_cols = [
        "GAME_ID",
        "PLAYER_ID",
        "TEAM_ID",
        "PTS",
        "REB",
        "AST",
        "STL",
        "BLK",
        "TOV",       # turnovers renamed from TO → TOV
        "MIN",
        "MATCHUP_TEAM_ID",  # opponent team ID replaces OPPONENT_TEAM_ID
    ]

    # Filter only the columns that exist (safe guard for small schema changes)
    available_cols = [c for c in expected_cols if c in df.columns]
    df = df[available_cols].copy()

    # Rename to normalized names for your DB
    rename_map = {
        "GAME_ID": "game_id",
        "PLAYER_ID": "player_id",
        "TEAM_ID": "team_id",
        "MATCHUP_TEAM_ID": "opponent_team_id",
        "MIN": "minutes",
        "PTS": "points",
        "REB": "rebounds",
        "AST": "assists",
        "STL": "steals",
        "BLK": "blocks",
        "TOV": "turnovers",
    }

    df.rename(columns=rename_map, inplace=True)

    return df

# ---------------------------------------------------------------------
# Database writes
# ---------------------------------------------------------------------
def upsert_games(df_games: pd.DataFrame):
    with get_connection() as conn:
        cursor = conn.cursor()
        sql = """
        INSERT OR REPLACE INTO games (
            game_id, season, game_date, home_team_id, away_team_id
        ) VALUES (
            :game_id, :season, :game_date, :home_team_id, :away_team_id
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


# ---------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------
def ingest_date(date_str: str):
    print(f"Fetching games for {date_str}...")
    games = fetch_games_for_date(date_str)

    if games.empty:
        print("No games to process.")
        return

    print(f"Found {len(games)} games. Inserting into DB...")
    upsert_games(games)

    print("Fetching boxscores...")
    for game_id in games["game_id"]:
        print(f"  - Game {game_id}")
        df_box = fetch_boxscores(game_id)
        insert_boxscores(df_box)
        time.sleep(0.7)

    print("Ingestion complete for", date_str)


if __name__ == "__main__":
    import sys

    init_db()

    if len(sys.argv) > 1:
        date_str = sys.argv[1]
    else:
        date_str = (datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")

    print("Running ingestion for", date_str)
    ingest_date(date_str)
