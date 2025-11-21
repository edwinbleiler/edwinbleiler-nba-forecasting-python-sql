# src/build_features_real.py

"""
build_features_real.py

Phase 4: Real feature engineering using actual boxscores from SQLite.

- Loads games + boxscores
- Computes DraftKings fantasy points
- Computes rolling stats (last 5 / 10 / 20 games)
- Computes DvP (defense vs player) at team level
- Saves processed features for modeling & projections
"""

from pathlib import Path
from datetime import datetime, timedelta

from db import get_connection, init_db

import pandas as pd
import numpy as np

from db import get_connection, init_db

BASE_DIR = Path(__file__).resolve().parents[1]
PROCESSED_DIR = BASE_DIR / "data" / "processed"
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)


def load_boxscores(last_n_seasons: int = 2) -> pd.DataFrame:
    """Load boxscores joined with games from SQLite for last N seasons (by date)."""
    init_db()
    with get_connection() as conn:
        df_games = pd.read_sql_query(
            "SELECT game_id, season, game_date, home_team_id, away_team_id FROM games",
            conn,
            parse_dates=["game_date"],
        )
        df_box = pd.read_sql_query(
            "SELECT game_id, player_id, team_id, opponent_team_id, "
            "minutes, points, rebounds, assists, steals, blocks, turnovers "
            "FROM boxscores",
            conn,
        )

    df = df_box.merge(df_games, on="game_id", how="left")

    # Filter last N seasons by date (approx by 2 calendar years)
    cutoff_date = df["game_date"].max() - timedelta(days=365 * last_n_seasons)
    df = df[df["game_date"] >= cutoff_date].copy()

    # Drop rows with obviously invalid minutes
    df = df[df["minutes"].notna()]
    df = df[df["minutes"] > 0]

    return df


def compute_fantasy_points_dk(df: pd.DataFrame) -> pd.Series:
    """
    DraftKings NBA scoring:
        PTS * 1
        + REB * 1.25
        + AST * 1.5
        + STL * 2
        + BLK * 2
        - TOV * 0.5
    """
    return (
        df["points"] * 1.0
        + df["rebounds"] * 1.25
        + df["assists"] * 1.5
        + df["steals"] * 2.0
        + df["blocks"] * 2.0
        - df["turnovers"] * 0.5
    )


def add_rolling_features(df: pd.DataFrame, windows=(5, 10, 20)) -> pd.DataFrame:
    """
    Add rolling averages over several windows:
    - fantasy points
    - minutes
    - points, rebounds, assists
    """

    df = df.sort_values(["player_id", "game_date"])

    for w in windows:
        grouped = df.groupby("player_id")

        df[f"fppg_last_{w}"] = (
            grouped["fantasy_points"].rolling(w).mean().reset_index(level=0, drop=True)
        )
        df[f"minutes_last_{w}"] = (
            grouped["minutes"].rolling(w).mean().reset_index(level=0, drop=True)
        )
        df[f"points_last_{w}"] = (
            grouped["points"].rolling(w).mean().reset_index(level=0, drop=True)
        )
        df[f"rebounds_last_{w}"] = (
            grouped["rebounds"].rolling(w).mean().reset_index(level=0, drop=True)
        )
        df[f"assists_last_{w}"] = (
            grouped["assists"].rolling(w).mean().reset_index(level=0, drop=True)
        )

        # Consistency: inverse of rolling stddev
        std = (
            grouped["fantasy_points"]
            .rolling(w)
            .std()
            .reset_index(level=0, drop=True)
        )
        df[f"consistency_last_{w}"] = 1 / (1 + std)

    # Usage proxy
    df["usage_proxy"] = (df["points"] + df["rebounds"] + df["assists"]) / df["minutes"]

    return df


def compute_dvp(df: pd.DataFrame, window_games: int = 20) -> pd.DataFrame:
    """
    Compute Defense vs Player (team-only version v1):
    - average fantasy points allowed by each opponent_team_id over last N games.

    Later, when we wire positions, this will become DvP by position.
    """
    df = df.sort_values(["opponent_team_id", "game_date"])

    df["fantasy_points_allowed"] = df["fantasy_points"]

    grouped = df.groupby("opponent_team_id")
    df["dvp_last_20"] = (
        grouped["fantasy_points_allowed"]
        .rolling(window_games)
        .mean()
        .reset_index(level=0, drop=True)
    )

    dvp = (
        df.groupby("opponent_team_id")["fantasy_points_allowed"]
        .mean()
        .reset_index()
        .rename(
            columns={
                "opponent_team_id": "defense_team_id",
                "fantasy_points_allowed": "dvp_overall_fp_allowed",
            }
        )
    )
    return df, dvp


def main():
    print("Loading boxscores + games from DB...")
    df = load_boxscores(last_n_seasons=2)

    if df.empty:
        print("No data found. Run ingestion first.")
        return

    print("Computing fantasy points (DraftKings)...")
    df["fantasy_points"] = compute_fantasy_points_dk(df)

    print("Adding rolling features...")
    df = add_rolling_features(df)

    print("Computing DvP (team-level)...")
    df, dvp = compute_dvp(df, window_games=20)

    # Save player-level features
    features_path = PROCESSED_DIR / "player_features_real.csv"
    dvp_path = PROCESSED_DIR / "dvp_real.csv"

    df.to_csv(features_path, index=False)
    dvp.to_csv(dvp_path, index=False)

    print(f"Saved player features to {features_path}")
    print(f"Saved dvp metrics to {dvp_path}")


if __name__ == "__main__":
    main()
