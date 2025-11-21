import subprocess
from fetch_schedule import get_today_games
from datetime import datetime, timedelta
import pytz

def schedule_runs():
    eastern = pytz.timezone("US/Eastern")
    now = datetime.now(eastern)

    games = get_today_games()
    for g in games:
        tip = datetime.fromisoformat(g["tipoff_utc"].replace("Z", "+00:00")).astimezone(eastern)
        run_time = tip - timedelta(minutes=30)

        if abs((run_time - now).total_seconds()) <= 3600:
            print(f"Triggering run for game at {tip}")
            subprocess.run([
                "gh", "workflow", "run", "run_projection.yaml",
                "-f", f"game_date={g['date']}",
                "-f", "minutes_before=30"
            ])

if __name__ == "__main__":
    schedule_runs()
