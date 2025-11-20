# src/model_minutes.py

"""
model_minutes.py

Trains a simple linear regression model to predict minutes played.
Saves the model to models/minutes_model.pkl
"""

from pathlib import Path

import pandas as pd
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split
import joblib

BASE_DIR = Path(__file__).resolve().parents[1]
PROCESSED_DIR = BASE_DIR / "data" / "processed"
MODELS_DIR = BASE_DIR / "models"
MODELS_DIR.mkdir(parents=True, exist_ok=True)


def main():
    data_path = PROCESSED_DIR / "model_dataset.csv"
    df = pd.read_csv(data_path, parse_dates=["game_date"])

    feature_cols = [
        "minutes_last_5",
        "minutes_last_10",
        "minutes_last_20",
        "fppg_last_10",
        "fppg_last_20",
        "usage_proxy",
        "dvp_last_20",
    ]

    X = df[feature_cols]
    y = df["minutes"]

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )

    model = LinearRegression()
    model.fit(X_train, y_train)

    train_score = model.score(X_train, y_train)
    test_score = model.score(X_test, y_test)

    print(f"Minutes model R^2 - train: {train_score:.3f}, test: {test_score:.3f}")

    model_path = MODELS_DIR / "minutes_model.pkl"
    joblib.dump(model, model_path)
    print(f"Saved minutes model to {model_path}")


if __name__ == "__main__":
    main()
