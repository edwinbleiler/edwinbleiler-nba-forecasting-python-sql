"""
fetch_data.py

Fetches NBA teams and players using nba_api and stores them in the SQLite database.
This is Phase 1: basic static data load (no games/boxscores yet).
"""

from typing import List, Dict

from nba_api.stats.static import teams as nba_teams
from nba_api.stats.static import players as nba_players

from db import get_connection, init_db


def fetch_teams() -> List[Dict]:
    """Fetch team metadata from nba_api."""
    return nba_teams.get_teams()


def fetch_players() -> List[Dict]:
    """Fetch player metadata from nba_api."""
    return nba_players.get_players()


def upsert_teams() -> None:
    """Insert or update teams in the database."""
    teams = fetch_teams()
    with get_connection() as conn:
        cursor = conn.cursor()
        sql = """
        INSERT OR REPLACE INTO teams (
            team_id,
            team_name,
            team_abbrev,
            team_nickname,
            team_city
        ) VALUES (
            :id,
            :full_name,
            :abbreviation,
            :nickname,
            :city
        );
        """
        cursor.executemany(sql, teams)
        conn.commit()
    print(f"Upserted {len(teams)} teams.")


def upsert_players() -> None:
    """Insert or update players in the database."""
    players = fetch_players()
    with get_connection() as conn:
        cursor = conn.cursor()
        sql = """
        INSERT OR REPLACE INTO players (
            player_id,
            full_name,
            first_name,
            last_name,
            is_active,
            team_id
        ) VALUES (
            :id,
            :full_name,
            :first_name,
            :last_name,
            :is_active,
            NULL
        );
        """
        cursor.executemany(sql, players)
        conn.commit()
    print(f"Upserted {len(players)} players.")


def main() -> None:
    """Initialize the DB and load static reference data."""
    init_db()
    upsert_teams()
    upsert_players()
    print("Static data fetch complete.")


if __name__ == "__main__":
    main()
