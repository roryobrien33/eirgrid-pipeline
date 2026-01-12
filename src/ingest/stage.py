import sqlite3
from pathlib import Path
from datetime import datetime, timezone

def stage_readings(df_tidy, source="smartgriddashboard_api"):
    # Investigate the path of this file
    INIT_DB_PATH = Path(__file__)

    # Move up the file path to the correct folder
    PROJECT_ROOT = INIT_DB_PATH.resolve().parents[2]

    # Build the file path for the schema and database files
    DB_PATH = PROJECT_ROOT / "db" / "eirgrid.db"

    # Create file directory (if not exist)
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    # Check columns
    required_columns = {"ts_utc", "metric", "region", "value"}
    missing = required_columns - set(df_tidy.columns)
    if missing:
        raise ValueError(f"Missing required columns: {sorted(missing)}")

    # Check timezone awareness
    tzinfo = getattr(df_tidy["ts_utc"].dtype, "tz", None)
    if tzinfo is None:
        raise ValueError(
            "Column 'ts_utc' must be timezone-aware. "
            "Localize/convert first, e.g. df['ts_utc'] = df['ts_utc'].dt.tz_convert('UTC')"
        )

    # Check it's actually UTC
    # (tzinfo can be a zoneinfo/pytz timezone object; stringifying is a simple robust check)
    if str(tzinfo).upper() not in ("UTC", "UTC+00:00"):
        raise ValueError(
            f"Column 'ts_utc' must be in UTC (found {tzinfo}). "
            "Convert with: df['ts_utc'] = df['ts_utc'].dt.tz_convert('UTC')"
        )

    # safe even if already UTC
    ts_utc_str = (
        df_tidy["ts_utc"]
        .dt.tz_convert("UTC")
        .dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    )

    ingested_at_str = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    # Build rows in the exact insert order
    rows_df = df_tidy.assign(
        ts_utc_str=ts_utc_str,
        source=source,
        ingested_at=ingested_at_str,
    )[["ts_utc_str", "metric", "region", "value", "source", "ingested_at"]]

    rows = list(rows_df.itertuples(index=False, name=None))
    row_count = len(rows)

    # Connect to DB
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("PRAGMA foreign_keys = ON;")
        sql = """
            INSERT INTO stg_readings
              (ts_utc, metric_code, region_code, value, source, ingested_at)
            VALUES (?, ?, ?, ?, ?, ?);
        """
        conn.executemany(sql, rows)

    return row_count


# Test
if __name__ == "__main__":
    import pandas as pd
    from pandas import Timestamp

    df_tidy = pd.DataFrame({
        "ts_utc": [Timestamp("2025-10-23 23:00:00", tz="UTC"),
                   Timestamp("2025-10-23 23:15:00", tz="UTC")],
        "metric": ["wind_actual", "wind_actual"],
        "region": ["ALL", "ALL"],
        "value": [2648.0, 2700.0],
        # extra columns are fine; they’ll be ignored by our selection
        "unit": ["MW", "MW"],
        "ts_local": [Timestamp("2025-10-24 00:00:00", tz="Europe/Dublin"),
                     Timestamp("2025-10-24 00:15:00", tz="Europe/Dublin")],
    })

    rows_added = stage_readings(df_tidy)
    print(f"Inserted {rows_added} rows into staging")

    PROJECT_ROOT = Path(__file__).resolve().parents[2]
    DB_PATH = PROJECT_ROOT / "db" / "eirgrid.db"
    with sqlite3.connect(DB_PATH) as conn:
        n = conn.execute("SELECT COUNT(*) FROM stg_readings;").fetchone()[0]
        print("stg_readings row count:", n)
        print(conn.execute("SELECT * FROM stg_readings ORDER BY ts_utc LIMIT 3;").fetchall())
        print("TEST COMPLETE")