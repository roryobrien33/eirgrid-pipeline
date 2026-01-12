import sys
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo
from pathlib import Path
import argparse

from ingest.fetch_data import fetch_one_day, DEFAULT_AREAS
from ingest.promote import get_conn, promote_day_delete_insert

# ---------------------------------------------------------------------
# Paths / DB location
# ---------------------------------------------------------------------

# This file lives in: project_root/src/pipeline/daily_runner.py
INIT_DB_PATH = Path(__file__)
PROJECT_ROOT = INIT_DB_PATH.resolve().parents[2]
DB_PATH = PROJECT_ROOT / "db" / "eirgrid.db"
DB_PATH.parent.mkdir(parents=True, exist_ok=True)

TZ_DUBLIN = ZoneInfo("Europe/Dublin")

# ---------------------------------------------------------------------

def get_yesterday_local(tz: ZoneInfo = TZ_DUBLIN):
    now_local = datetime.now(tz)
    today_local = now_local.date()
    yesterday_local = today_local - timedelta(days=1)
    return yesterday_local

def run_daily_pipeline(daily_date: date | None = None):
    """
    Run the daily ETL pipeline for a given local Dublin date.
    If no date is provided, defaults to yesterday in Europe/Dublin.
    """

    # 1. Determine which day to run
    if daily_date is None:
        daily_date = get_yesterday_local()

    if not isinstance(daily_date, date):
        raise TypeError("daily_date must be a datetime.date or None.")

    print(f"Running daily pipeline for {daily_date}...")

    # 2. Fetch + stage
    rows_staged = fetch_one_day(daily_date, DEFAULT_AREAS)

    if rows_staged == 0:
        print(f"No rows staged for {daily_date}, skipping promotion.")
        return 0

    # 3. Promote canonical slice
    with get_conn() as conn:
        rows_inserted = promote_day_delete_insert(conn, daily_date)

    print(f"Inserted {rows_inserted} canonical rows into fact_readings.")
    print("Pipeline complete.")

    return rows_inserted


def main(argv: list[str] | None = None):
    parser = argparse.ArgumentParser(description="Run the EirGrid daily pipeline (fetch -> stage -> promote)")
    parser.add_argument("--day",
                        type=str,
                        default=None,
                        help= "Run pipeline for a specific date (YYYY-MM-DD).")

    args = parser.parse_args(argv)

    if args.day is None:
        run_daily_pipeline()
    else:
        try:
            day_local = date.fromisoformat(args.day)

        except ValueError:
            raise SystemExit(f"Invalid --day value: '{args.day}'. Use YYYY-MM-DD.")
        run_daily_pipeline(day_local)



if __name__ == "__main__":
    main()
    print("TEST COMPLETE")

