import subprocess
import sys

def run_cmd(cmd):
    print(f"\n>>> {cmd}")
    subprocess.run(cmd, shell=True, check=True)

def main(date_override=None):
    if date_override:
        ingest_date = date_override
    else:
        ingest_date = "yesterday"

    print(f"Running full pipeline for: {ingest_date}")

    run_cmd(f"python src/ingest_boxscores.py {ingest_date}")
    run_cmd("python src/build_features_real.py")
    run_cmd("python src/build_model_dataset.py")
    run_cmd("python src/model_minutes.py")

if __name__ == "__main__":
    date_override = sys.argv[1] if len(sys.argv) > 1 else None
    main(date_override)
