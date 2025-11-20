"""
build_features.py

Phase 2: Feature engineering for NBA player forecasting.

This script loads game-level boxscore data (once added in Phase 3),
calculates fantasy points, rolling averages, consistency metrics,
and opponent DvP factors.

For now (Phase 2), we simulate small samples so you can validate the pipeline.
"""

import sqlite3
from pathlib import Path
import pandas as pd
import numpy as np

from db import get_connection, init_db

BASE_DIR = Path(__file__).resolve().parents[1]
PROCESSED_DIR = BASE_DIR / "data" / "processed"
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)


# ---------------------------
# Fantasy scoring
# ---------------------------

def compute_fantasy_points(df: pd.DataFrame) -> pd.Series:
    """Compute fantasy points using standard scoring."""
    return (
        df["points"] * 1 +
        df["rebounds"] * 1.2 +
        df["assists"] * 1.5 +
        df["steals"] * 3 +
        df["blocks"] * 3 -
        df["turnovers"] * 1
    )


# ---------------------------
# Rolling averages and consistency
# ---------------------------

def add_rolling_features(df: pd.DataFrame) -> pd.DataFrame:
    """Compute rolling 10-game fantasy averages & consistency score."""
    
    df = df.sort_values(["player_id", "game_date"])

    # Rolling 10-game fantasy average
    df["fppg_last_10"] = (
        df.groupby("player_id")["fantasy_points"]
        .rolling(10)
        .mean()
        .reset_index(level=0, drop=True)
    )

    # Consistency score (rolling std dev)
    df["consistency_score"] = (
        df.groupby("player_id")["fantasy_points"]
        .rolling(10)
        .std()
        .reset_index(level=0, drop=True)
    )

    # Normalize so higher is better (invert std dev)
    df["consistency_score"] = 1 / (1 + df["consistency_score"])

    return df


# ---------------------------
# DvP metric (defense vs position)
# ---------------------------

def compute_dvp(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute Defense vs Position (DvP).
    Simplified version:
        For each defense team + player position:
            mean fantasy points allowed
    """
    dvp = (
        df.groupby(["opponent_team_id", "position"])["fantasy_points"]
        .mean()
        .rename("dvp")
        .reset_index()
    )
    return dvp


# ---------------------------
# Main build function
# ---------------------------

def build_features():
    init_db()  # ensure schema exists

    # In Phase 2 we simulate data.
    # In Phase 3 we'll load real boxscores from the API.

    print("Loading sample simulated data...")

    # Example sample data (you can replace with real later)
    data = {
        "player_id": [1, 1, 1, 2, 2],
        "player_name": ["Player A", "Player A", "Player A", "Player B", "Player B"],
        "position": ["SG", "SG", "SG", "PF", "PF"],
        "game_date": pd.to_datetime([
            "2024-01-01", "2024-01-03", "2024-01-05",
            "2024-01-01", "2024-01-03"
        ]),
        "team_id": [10, 10, 10, 20, 20],
        "opponent_team_id": [5, 2, 8, 7, 5],
        "points": [25, 18, 30, 12, 15],
        "rebounds": [4, 6, 3, 9, 8],
        "assists": [5, 7, 4, 2, 1],
        "steals": [1, 2, 0, 0, 1],
        "blocks": [0, 1, 2, 1, 0],
        "turnovers": [3, 1, 2, 2, 1]
    }

    df = pd.DataFrame(data)

    print("Computing fantasy points...")
    df["fantasy_points"] = compute_fantasy_points(df)

    print("Computing rolling averages & consistency...")
    df = add_rolling_features(df)

    print("Computing DvP...")
    dvp = compute_dvp(df)

    # Save outputs
    df.to_csv(PROCESSED_DIR / "player_features.csv", index=False)
    dvp.to_csv(PROCESSED_DIR / "dvp_metrics.csv", index=False)

    print("Feature engineering complete.")
    print(f"Saved: {PROCESSED_DIR}/player_features.csv")
    print(f"Saved: {PROCESSED_DIR}/dvp_metrics.csv")


if __name__ == "__main__":
    build_features()
