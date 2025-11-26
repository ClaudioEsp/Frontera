# orchestrator/run_daily_pipeline.py

import sys
import subprocess
import argparse
from datetime import datetime


JOBS_WITH_DATE = [
    "orchestrator.jobs.get_routes",
    "orchestrator.jobs.get_details_from_route",
    "orchestrator.jobs.get_dispatches",
]

JOBS_NO_DATE = [
    "orchestrator.jobs.backfill_tipo_orden_from_tags",
    "orchestrator.jobs.backfill_promise_date_from_tags",
    "orchestrator.jobs.get_ct",
    "orchestrator.jobs.get_substatus",
]


def run_job(module: str, date_str: str | None = None) -> None:
    """
    Run a single job as:
      python -m <module> [--date YYYY-MM-DD]
    """
    cmd = [sys.executable, "-m", module]
    if date_str is not None:
        cmd.extend(["--date", date_str])

    print(f"\n>>> Running: {' '.join(cmd)}")
    result = subprocess.run(cmd)

    if result.returncode != 0:
        raise SystemExit(
            f"Job {module} failed with exit code {result.returncode}. "
            "Aborting pipeline."
        )
    print(f"✓ Finished: {module}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run daily DispatchTrack ETL pipeline."
    )
    parser.add_argument(
        "--date",
        required=True,
        help="Date to process (YYYY-MM-DD)",
    )
    return parser.parse_args()


def validate_date(date_str: str) -> str:
    # Just to be sure format is correct
    try:
        datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        raise SystemExit(f"Invalid date format: {date_str}. Use YYYY-MM-DD.")
    return date_str


def main() -> None:
    args = parse_args()
    date_str = validate_date(args.date)

    # 1) Jobs that need --date
    for module in JOBS_WITH_DATE:
        run_job(module, date_str=date_str)

    # 2) Jobs that do NOT need --date
    for module in JOBS_NO_DATE:
        run_job(module, date_str=None)

    print("\n🎉 Pipeline completed successfully.")


if __name__ == "__main__":
    main()
