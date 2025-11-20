# src/build_model_dataset.py

"""
build_model_dataset.py

Prepares a modeling dataset from player_features_real.csv.

- Filters to rows with full stats
- Selects relevant features
- Defines targets:
    - minutes
    - points
    - rebounds
    - assists
    - fantasy_points
- Saves to data/processed/model_dataset.csv
"""

from pathlib import Path

import pandas as pd

BASE_DIR = Path(__file__).resolve().parents[1]
PROCESSED_DIR = BASE_DIR / "data" / "processed"


def main():
    features_path = PROCESSED_DIR / "player_features_real.csv"
    df = pd.read_csv(features_path, parse_dates=["game_date"])

    # Drop rows with missing critical values
    df = df.dropna(
        subset=[
            "minutes",
            "points",
            "rebounds",
            "assists",
            "fantasy_points",
            "fppg_last_10",
            "minutes_last_10",
        ]
    )

    # Example feature set (can be expanded later)
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

    target_cols = ["minutes", "points", "rebounds", "assists", "fantasy_points"]

    # Keep IDs + game context
    id_cols = ["player_id", "team_id", "opponent_team_id", "game_id", "game_date"]

    # Drop rows with NaNs in selected feature columns
    df_model = df[id_cols + feature_cols + target_cols].dropna()

    model_path = PROCESSED_DIR / "model_dataset.csv"
    df_model.to_csv(model_path, index=False)

    print(f"Saved model dataset to {model_path}")


if __name__ == "__main__":
    main()
