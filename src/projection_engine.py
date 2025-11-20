# src/projection_engine.py

"""
projection_engine.py

Loads trained models and latest player features,
generates projections for a given date (default: today),
and saves them under projections/YYYY-MM-DD/projections.csv.
"""

from pathlib import Path
from datetime import datetime
import os

import pandas as pd
import joblib

BASE_DIR = Path(__file__).resolve().parents[1]
PROCESSED_DIR = BASE_DIR / "data" / "processed"
MODELS_DIR = BASE_DIR / "models"
PROJECTIONS_DIR = BASE_DIR / "projections"


def load_latest_features() -> pd.DataFrame:
    path = PROCESSED_DIR / "player_features_real.csv"
    df = pd.read_csv(path, parse_dates=["game_date"])

    # For each player, keep the most recent game row as "current state"
    df = df.sort_values(["player_id", "game_date"])
    latest = df.groupby("player_id").tail(1).copy()
    return latest


def main(target_date: str | None = None):
    if target_date is None:
        target_date = datetime.today().strftime("%Y-%m-%d")

    print(f"Generating projections for {target_date}...")

    df = load_latest_features()

    # Load models
    minutes_model = joblib.load(MODELS_DIR / "minutes_model.pkl")
    points_model = joblib.load(MODELS_DIR / "points_model.pkl")
    rebounds_model = joblib.load(MODELS_DIR / "rebounds_model.pkl")
    assists_model = joblib.load(MODELS_DIR / "assists_model.pkl")
    fantasy_model = joblib.load(MODELS_DIR / "fantasy_model.pkl")

    feature_cols = [
        "minutes_last_5",
        "minutes_last_10",
        "minutes_last_20",
        "fppg_last_5",
        "fppg_last_10",
        "fppg_last_20",
        "points_last_10",
        "rebounds_last_10",
        "assists_last_10",
        "usage_proxy",
        "dvp_last_20",
    ]

    X = df[feature_cols]

    df["proj_minutes"] = minutes_model.predict(X)
    df["proj_points"] = points_model.predict(X)
    df["proj_rebounds"] = rebounds_model.predict(X)
    df["proj_assists"] = assists_model.predict(X)
    df["proj_fantasy_points"] = fantasy_model.predict(X)

    # Save projections
    out_dir = PROJECTIONS_DIR / target_date
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "projections.csv"

    keep_cols = [
        "player_id",
        "team_id",
        "opponent_team_id",
        "proj_minutes",
        "proj_points",
        "proj_rebounds",
        "proj_assists",
        "proj_fantasy_points",
    ]

    df[keep_cols].to_csv(out_path, index=False)
    print(f"Saved projections to {out_path}")


if __name__ == "__main__":
    import sys

    date_arg = sys.argv[1] if len(sys.argv) > 1 else None
    main(date_arg)
