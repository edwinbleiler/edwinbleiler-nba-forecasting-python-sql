import pandas as pd
import numpy as np
from pathlib import Path
from db import get_connection
import math

# ---------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------

BASE_DIR = Path(__file__).resolve().parents[1]
STATIC_PATH = BASE_DIR / "data" / "static" / "team_locations.csv"
FEATURE_OUTPUT = BASE_DIR / "outputs" / "features.csv"

# Ensure directories exist
STATIC_PATH.parent.mkdir(parents=True, exist_ok=True)
FEATURE_OUTPUT.parent.mkdir(parents=True, exist_ok=True)

# ---------------------------------------------------------------------
# 1. Utility Functions
# ---------------------------------------------------------------------

def clean_minutes(min_str):
    """Convert weird NBA minutes formats to float."""
    if min_str is None:
        return 0.0
    if isinstance(min_str, (int, float)):
        return float(min_str)
    if ":" in min_str:
        m, s = min_str.split(":")
        return float(m) + float(s)/60
    try:
        return float(min_str)
    except:
        return 0.0

def haversine(lat1, lon1, lat2, lon2):
    """Compute great-circle distance between two lat/lon points."""
    R = 6371  # km
    p = math.pi / 180
    a = (0.5 - math.cos((lat2 - lat1)*p)/2
         + math.cos(lat1*p) * math.cos(lat2*p)
         * (1 - math.cos((lon2 - lon1)*p)) / 2)
    return 2 * R * math.asin(math.sqrt(a))

# Fantasy scoring (DK)
def dk_fp(row):
    return (
        row["points"] +
        1.25 * row["rebounds"] +
        1.5 * row["assists"] +
        2 * row["steals"] +
        2 * row["blocks"] -
        0.5 * row["turnovers"]
    )

# ---------------------------------------------------------------------
# 2. Load Team Location Data
# ---------------------------------------------------------------------

def load_team_locations():
    if not STATIC_PATH.exists():
        raise FileNotFoundError(
            f"Missing team_locations.csv at {STATIC_PATH}. "
            f"Create it using: data/static/team_locations.csv"
        )
    df = pd.read_csv(STATIC_PATH)
    return df.set_index("team_id")

# ---------------------------------------------------------------------
# 3. Main Build Function
# ---------------------------------------------------------------------

def build_features():
    print("Loading raw DB data...")

    with get_connection() as conn:
        df_games = pd.read_sql_query(
            """
            SELECT game_id, game_date, home_team_id, away_team_id
            FROM games
            ORDER BY game_date
            """,
            conn,
            parse_dates=["game_date"]
        )

        df_box = pd.read_sql_query(
            """
            SELECT game_id, player_id, team_id, opponent_team_id,
                   minutes, points, rebounds, assists, steals,
                   blocks, turnovers
            FROM boxscores
            """,
            conn
        )

    if df_games.empty or df_box.empty:
        print("No data available. Run ingestion first.")
        return

    print("Cleaning data...")

    # Clean minutes
    df_box["minutes"] = df_box["minutes"].apply(clean_minutes)

    # Add DK fantasy points
    df_box["dk_fp"] = df_box.apply(dk_fp, axis=1)

    # Merge game info
    df = df_box.merge(df_games, on="game_id", how="left")

    # -----------------------------------------------------------------
    # 4. Compute Rest & Travel Features
    # -----------------------------------------------------------------

    print("Computing rest & travel...")

    team_locs = load_team_locations()

    # Last game date per team/player
    df = df.sort_values(["player_id", "game_date"])

    df["prev_game_date"] = df.groupby("player_id")["game_date"].shift(1)
    df["days_rest"] = (df["game_date"] - df["prev_game_date"]).dt.days

    # Rest flags
    df["is_b2b"] = (df["days_rest"] == 1).astype(int)
    df["is_3in4"] = (df["days_rest"].between(1, 2)).astype(int)

    # Travel distance
    def compute_travel(row):
        team = row["team_id"]
        prev = row.get("prev_game_id", None)
        if pd.isna(prev):
            return 0

        # Home team or away?
        game_row = df_games[df_games["game_id"] == row["game_id"]].iloc[0]
        loc = "home" if game_row.home_team_id == team else "away"

        prev_game_row = df_games[df_games["game_id"] == row["prev_game_id"]]
        if prev_game_row.empty:
            return 0
        prev_game_row = prev_game_row.iloc[0]
        prev_loc = "home" if prev_game_row.home_team_id == team else "away"

        # Current coords
        cur_team_id = team
        cur_lat = team_locs.loc[cur_team_id, "lat"]
        cur_lon = team_locs.loc[cur_team_id, "lon"]

        prev_lat = cur_lat
        prev_lon = cur_lon

        # Real travel: from previous arena → today’s arena
        if loc == "home":
            cur_lat = team_locs.loc[team, "lat"]
            cur_lon = team_locs.loc[team, "lon"]
        else:
            cur_lat = team_locs.loc[row["opponent_team_id"], "lat"]
            cur_lon = team_locs.loc[row["opponent_team_id"], "lon"]

        if prev_loc == "home":
            prev_lat = team_locs.loc[team, "lat"]
            prev_lon = team_locs.loc[team, "lon"]
        else:
            prev_lat = team_locs.loc[prev_game_row.opponent_team_id, "lat"]
            prev_lon = team_locs.loc[prev_game_row.opponent_team_id, "lon"]

        return haversine(prev_lat, prev_lon, cur_lat, cur_lon)

    df["prev_game_id"] = df.groupby("player_id")["game_id"].shift(1)
    df["travel_km"] = df.apply(compute_travel, axis=1)

    # -----------------------------------------------------------------
    # 5. Compute Rolling Averages
    # -----------------------------------------------------------------

    print("Computing rolling stats...")

    df = df.sort_values(["player_id", "game_date"])

    roll_fields = ["minutes", "points", "rebounds", "assists",
                   "steals", "blocks", "turnovers", "dk_fp"]

    for w in [5, 10]:
        for col in roll_fields:
            df[f"{col}_roll{w}"] = (
                df.groupby("player_id")[col]
                .rolling(w, min_periods=1)
                .mean()
                .reset_index(level=0, drop=True)
            )

    # -----------------------------------------------------------------
    # 6. Opponent Allowed (DvP metrics)
    # -----------------------------------------------------------------

    print("Computing opponent-allowed features...")

    opp = df.groupby(["opponent_team_id", "game_date"]).agg({
        "points": "sum",
        "rebounds": "sum",
        "assists": "sum",
        "steals": "sum",
        "blocks": "sum",
        "turnovers": "sum",
        "dk_fp": "sum"
    }).reset_index()

    opp = opp.sort_values(["opponent_team_id", "game_date"])

    for w in [5, 10]:
        for col in ["points", "rebounds", "assists", "steals", "blocks", "turnovers", "dk_fp"]:
            opp[f"opp_{col}_allowed_roll{w}"] = (
                opp.groupby("opponent_team_id")[col]
                .rolling(w, min_periods=1)
                .mean()
                .reset_index(level=0, drop=True)
            )

    df = df.merge(
        opp.drop(columns=["points", "rebounds", "assists",
                          "steals", "blocks", "turnovers", "dk_fp"]),
        on=["opponent_team_id", "game_date"],
        how="left"
    )

    # -----------------------------------------------------------------
    # 7. Save Output
    # -----------------------------------------------------------------

    print(f"Saving features to {FEATURE_OUTPUT} ...")
    df.to_csv(FEATURE_OUTPUT, index=False)

    print("Feature build complete!")


# ---------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------

if __name__ == "__main__":
    build_features()
