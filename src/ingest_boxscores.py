"""
ingest_boxscores.py

Phase 3: Ingest real NBA game data & boxscores into SQLite.

Fixes:
- ScoreboardV2 is unreliable (returns HTML, empty JSON, or rate limits)
- This version uses direct scoreboardv3 endpoint with real browser headers

Tables populated:
    - games
    - boxscores
"""

import time
from datetime import datetime, timedelta
from pathlib import Path
import requests
import pandas as pd

# Still use nba_api for boxscores
from nba_api.stats.endpoints import BoxScoreTraditionalV2
from nba_api.stats.library.http import NBAStatsHTTP

# Fake browser headers to bypass NBA blocking
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
# Fetch games (replaces ScoreboardV2 — this version is reliable)
# ---------------------------------------------------------------------
def fetch_games_for_date(date_str: str) -> pd.DataFrame:
    """
    Fetch games directly using NBA Stats API (scoreboardv3).
    date_str: YYYY-MM-DD
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

    params = {
        "GameDate": date_str,
        "LeagueID": "00",
        "DayOffset": "0"
    }

    resp = requests.get(url, headers=headers, params=params, timeout=10)

    if resp.status_code != 200:
        print("ERROR: status code", resp.status_code)
        print(resp.text[:200])
        return pd.DataFrame()

    data = resp.json()
    games_list = data.get("scoreboard", {}).get("games", [])

    if not games_list:
        print("No games found for", date_str)
        return pd.DataFrame()

    rows = []
    for g in games_list:
        rows.append({
            "game_id": g.get("gameId"),
            "season": g.get("season"),
            "game_date": g.get("gameDateEst"),
            "home_team_id": g.get("homeTeam", {}).get("teamId"),
            "away_team_id": g.get("awayTeam", {}).get("teamId")
        })

    return pd.DataFrame(rows)

# ---------------------------------------------------------------------
# Fetch boxscores — nba_api still works for this
# ---------------------------------------------------------------------
def fetch_boxscores(game_id: str) -> pd.DataFrame:
    """
    Fetch boxscores for a single game using BoxScoreTraditionalV2.
    """

    box = BoxScoreTraditionalV2(game_id=game_id)
    df = box.get_data_frames()[0]

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

# ---------------------------------------------------------------------
# Database Writes
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
           


