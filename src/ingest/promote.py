# src/ingest/promote.py

import sqlite3
from datetime import timedelta, datetime, date
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd

# ---------------------------------------------------------------------
# Paths / DB location
# ---------------------------------------------------------------------

# This file lives in: project_root/src/ingest/promote.py
INIT_DB_PATH = Path(__file__)
PROJECT_ROOT = INIT_DB_PATH.resolve().parents[2]
DB_PATH = PROJECT_ROOT / "db" / "eirgrid.db"
DB_PATH.parent.mkdir(parents=True, exist_ok=True)


# ---------------------------------------------------------------------
# Connection + dimension lookups
# ---------------------------------------------------------------------

def get_conn() -> sqlite3.Connection:
    """
    Open a SQLite connection to our project DB and enable foreign keys.

    Usage:
        with get_conn() as conn:
            ...
    """
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


def get_dim_maps(conn: sqlite3.Connection):
    """
    Read dimension tables and build quick lookup maps:

      metric_map: metric_code -> metric_id
      region_map: region_code -> region_id

    These are used to convert the canonical slice from metric_code/region_code
    to the integer IDs stored in fact_readings.
    """
    cur = conn.cursor()

    cur.execute("SELECT metric_id, metric_code FROM dim_metric ORDER BY metric_code;")
    metric_rows = cur.fetchall()

    cur.execute("SELECT region_id, region_code FROM dim_region ORDER BY region_code;")
    region_rows = cur.fetchall()

    metric_map = {code: mid for (mid, code) in metric_rows}
    region_map = {code: rid for (rid, code) in region_rows}

    return metric_map, region_map


# ---------------------------------------------------------------------
# Expected slots for a local calendar day (handles DST)
# ---------------------------------------------------------------------

def promote_complete_days(day_local: date) -> int:
    """
    For a given local calendar day (Europe/Dublin), return the expected
    number of quarter-hour slots.

    Usually 96, but DST transitions can produce 92 or 100. We derive this
    by actually generating the timestamps.
    """
    tz = ZoneInfo("Europe/Dublin")

    start_local = datetime(
        year=day_local.year,
        month=day_local.month,
        day=day_local.day,
        hour=0,
        minute=0,
        second=0,
        tzinfo=tz,
    )
    end_local = start_local + timedelta(days=1)

    timestamps = pd.date_range(
        start=start_local,
        end=end_local,
        freq="15min",
        inclusive="left",
    )

    expected_slots = len(timestamps)
    return expected_slots


# ---------------------------------------------------------------------
# Diagnostics: staging row counts per day (including duplicates)
# ---------------------------------------------------------------------

def count_staging_rows_for_day(conn: sqlite3.Connection, day_local: date) -> pd.DataFrame:
    """
    For a given local day, count *all* staging rows (including duplicates)
    per (metric_code, region_code) in stg_readings.

    Returns a DataFrame with:
        metric_code, region_code, rows
    """
    if not isinstance(day_local, date):
        raise ValueError(f"{day_local!r} is not a datetime.date")

    # Build local day bounds
    tz = ZoneInfo("Europe/Dublin")
    start_local = datetime(day_local.year, day_local.month, day_local.day, 0, 0, 0, tzinfo=tz)
    end_local = start_local + timedelta(days=1)

    # Convert to UTC ISO strings (same format as stored in ts_utc)
    start_utc = start_local.astimezone(ZoneInfo("UTC")).strftime("%Y-%m-%dT%H:%M:%SZ")
    end_utc = end_local.astimezone(ZoneInfo("UTC")).strftime("%Y-%m-%dT%H:%M:%SZ")

    sql = """
        SELECT
            m.metric_code,
            r.region_code,
            COUNT(*) AS rows
        FROM stg_readings AS sr
        JOIN dim_metric AS m ON m.metric_code = sr.metric_code
        JOIN dim_region AS r ON r.region_code = sr.region_code
        WHERE sr.ts_utc >= ?
          AND sr.ts_utc <  ?
        GROUP BY m.metric_code, r.region_code
        ORDER BY m.metric_code, r.region_code;
    """
    cur = conn.cursor()
    cur.execute(sql, (start_utc, end_utc))
    rows = cur.fetchall()

    df = pd.DataFrame(rows, columns=["metric_code", "region_code", "rows"])
    return df


# ---------------------------------------------------------------------
# Completeness check: distinct UTC slot coverage per metric/region
# ---------------------------------------------------------------------

def distinct_slot_coverage(conn: sqlite3.Connection, day_local: date) -> pd.DataFrame:
    """
    For a given local day, count DISTINCT ts_utc values per (metric_code, region_code)
    in stg_readings, then compare against the expected number of slots.

    Returns a DataFrame with:
        metric_code, region_code, distinct_slots, expected_slots, missing, is_complete
    """
    if not isinstance(day_local, date):
        raise ValueError(f"{day_local!r} is not a datetime.date")

    # 1) Build local day bounds and convert to UTC ISO
    tz = ZoneInfo("Europe/Dublin")
    start_local = datetime(day_local.year, day_local.month, day_local.day, 0, 0, 0, tzinfo=tz)
    end_local = start_local + timedelta(days=1)

    start_utc = start_local.astimezone(ZoneInfo("UTC")).strftime("%Y-%m-%dT%H:%M:%SZ")
    end_utc = end_local.astimezone(ZoneInfo("UTC")).strftime("%Y-%m-%dT%H:%M:%SZ")

    # 2) Count DISTINCT UTC timestamps by metric/region in staging
    sql = """
        SELECT
            sr.metric_code,
            sr.region_code,
            COUNT(DISTINCT sr.ts_utc) AS distinct_slots
        FROM stg_readings AS sr
        WHERE sr.ts_utc >= ?
          AND sr.ts_utc <  ?
        GROUP BY sr.metric_code, sr.region_code
        ORDER BY sr.metric_code, sr.region_code;
    """
    cur = conn.cursor()
    cur.execute(sql, (start_utc, end_utc))
    rows = cur.fetchall()

    df = pd.DataFrame(rows, columns=["metric_code", "region_code", "distinct_slots"])

    # 3) Completeness math
    expected = promote_complete_days(day_local)  # e.g., 96 (or 92/100 on DST days)
    df["expected_slots"] = expected
    df["missing"] = df["expected_slots"] - df["distinct_slots"]
    df["is_complete"] = df["distinct_slots"] == df["expected_slots"]

    return df


# ---------------------------------------------------------------------
# Build canonical slice for one complete day
# ---------------------------------------------------------------------

def build_canonical_slice_for_day(conn: sqlite3.Connection, day_local: date) -> pd.DataFrame:
    """
    For a given local calendar day (Europe/Dublin):

      1) Validate that all metric/region combos have a full set of 15-min slots
         (using distinct_slot_coverage).
      2) Pull ALL staging rows for that UTC window from stg_readings.
      3) For each (ts_utc, metric_code, region_code), keep only the latest ingested_at.
      4) Map metric_code/region_code -> metric_id/region_id using dimension tables.
      5) Return a canonical DataFrame with columns: ts_utc, metric_id, region_id, value.

    This does NOT insert into fact_readings; it just returns the slice ready to insert.
    """
    if not isinstance(day_local, date):
        raise ValueError(f"{day_local!r} is not a datetime.date")

    # --- 1) Build local bounds & convert to UTC ISO strings ---
    tz_local = ZoneInfo("Europe/Dublin")
    start_local = datetime(
        year=day_local.year,
        month=day_local.month,
        day=day_local.day,
        hour=0,
        minute=0,
        second=0,
        tzinfo=tz_local,
    )
    end_local = start_local + timedelta(days=1)

    start_utc = start_local.astimezone(ZoneInfo("UTC")).strftime("%Y-%m-%dT%H:%M:%SZ")
    end_utc = end_local.astimezone(ZoneInfo("UTC")).strftime("%Y-%m-%dT%H:%M:%SZ")

    # --- 2) Check completeness using distinct_slot_coverage ---
    coverage = distinct_slot_coverage(conn, day_local)
    if not coverage["is_complete"].all():
        incomplete = coverage[~coverage["is_complete"]]
        raise RuntimeError(
            f"Incomplete day {day_local}: some metric/region combos have missing slots.\n"
            f"{incomplete}"
        )

    # --- 3) Pull all staging rows for that UTC window ---
    sql = """
        SELECT
            ts_utc,
            metric_code,
            region_code,
            value,
            ingested_at,
            source
        FROM stg_readings
        WHERE ts_utc >= ?
          AND ts_utc <  ?
    """
    cur = conn.cursor()
    cur.execute(sql, (start_utc, end_utc))
    rows = cur.fetchall()

    if not rows:
        # This "shouldn't" happen if coverage says complete, but be defensive.
        return pd.DataFrame(columns=["ts_utc", "metric_id", "region_id", "value"])

    df = pd.DataFrame(
        rows,
        columns=["ts_utc", "metric_code", "region_code", "value", "ingested_at", "source"],
    )

    # --- 4) Parse timestamps & sort so "latest ingested_at wins" ---
    df["ts_utc"] = pd.to_datetime(df["ts_utc"], utc=True, errors="raise")
    df["ingested_at"] = pd.to_datetime(df["ingested_at"], utc=True, errors="coerce")

    # Drop any rows with bad ingested_at (shouldn't usually happen)
    df = df.dropna(subset=["ingested_at"])

    # Sort so that the newest ingested_at is last within each group
    df = df.sort_values(["ts_utc", "metric_code", "region_code", "ingested_at"])

    # --- 5) Deduplicate by (ts_utc, metric_code, region_code) keeping latest ingested_at ---
    df_latest = (
        df.groupby(["ts_utc", "metric_code", "region_code"], as_index=False)
          .tail(1)
          .reset_index(drop=True)
    )

    # --- 6) Ensure value is numeric and non-null ---
    df_latest["value"] = pd.to_numeric(df_latest["value"], errors="coerce")
    if df_latest["value"].isna().any():
        bad = df_latest[df_latest["value"].isna()]
        raise RuntimeError(
            f"Found NULL/invalid values in canonical slice for {day_local}:\n{bad}"
        )

    # --- 7) Map codes -> IDs using dimension tables ---
    metric_map, region_map = get_dim_maps(conn)

    df_latest["metric_id"] = df_latest["metric_code"].map(metric_map)
    df_latest["region_id"] = df_latest["region_code"].map(region_map)

    if df_latest["metric_id"].isna().any():
        unknown = df_latest[df_latest["metric_id"].isna()][["metric_code"]].drop_duplicates()
        raise RuntimeError(f"Unknown metric_code(s) when mapping to IDs: {unknown}")

    if df_latest["region_id"].isna().any():
        unknown = df_latest[df_latest["region_id"].isna()][["region_code"]].drop_duplicates()
        raise RuntimeError(f"Unknown region_code(s) when mapping to IDs: {unknown}")

    # --- 8) Return canonical shape for fact_readings ---
    canonical = df_latest[["ts_utc", "metric_id", "region_id", "value"]].copy()
    return canonical


# ---------------------------------------------------------------------
# Promotion operation: delete + insert (idempotent for a day)
# ---------------------------------------------------------------------

def promote_day_delete_insert(conn: sqlite3.Connection, day_local: date) -> int:
    """
    Idempotent promotion for a single local calendar day:

      1) Build canonical slice for the day (will raise if incomplete).
      2) Delete any existing fact_readings rows for that day's UTC window.
      3) Insert the canonical rows with a fresh ingested_at timestamp.

    Returns the number of rows inserted into fact_readings.
    """
    if not isinstance(day_local, date):
        raise ValueError(f"{day_local!r} is not a datetime.date")

    # Step 1: build canonical slice
    canon = build_canonical_slice_for_day(conn, day_local)
    if canon.empty:
        print(f"[promote] {day_local} → no canonical rows (empty). Skipping.")
        return 0

    # Step 2: compute UTC window and delete existing rows for that day
    tz_local = ZoneInfo("Europe/Dublin")
    start_local = datetime(day_local.year, day_local.month, day_local.day, 0, 0, 0, tzinfo=tz_local)
    end_local = start_local + timedelta(days=1)

    start_utc = start_local.astimezone(ZoneInfo("UTC")).strftime("%Y-%m-%dT%H:%M:%SZ")
    end_utc = end_local.astimezone(ZoneInfo("UTC")).strftime("%Y-%m-%dT%H:%M:%SZ")

    cur = conn.cursor()
    cur.execute(
        """
        DELETE FROM fact_readings
        WHERE ts_utc >= ?
          AND ts_utc <  ?;
        """,
        (start_utc, end_utc),
    )

    # Step 3: prepare rows to insert (with new ingested_at)
    now_utc = datetime.now(tz=ZoneInfo("UTC")).strftime("%Y-%m-%dT%H:%M:%SZ")

    rows_to_insert = []
    for ts, metric_id, region_id, value in canon.to_records(index=False):
        ts_iso = ts.isoformat().replace("+00:00", "Z")  # match ts_utc TEXT format in DB
        rows_to_insert.append((ts_iso, int(metric_id), int(region_id), float(value), now_utc))

    insert_sql = """
        INSERT INTO fact_readings (
            ts_utc,
            metric_id,
            region_id,
            value,
            ingested_at
        )
        VALUES (?, ?, ?, ?, ?);
    """
    cur.executemany(insert_sql, rows_to_insert)
    conn.commit()

    inserted = len(rows_to_insert)
    print(f"[promote] {day_local} → inserted {inserted} canonical rows into fact_readings")
    return inserted


def promote_range_delete_insert(conn: sqlite3.Connection, start_date: date, end_date: date) -> dict:
    if not isinstance(start_date, date) or not isinstance(end_date, date):
        raise ValueError("start_date and end_date must be datetime.date")
    if start_date > end_date:
        raise ValueError("start_date must be <= end_date")

    n_days = (end_date - start_date).days + 1

    days_attempted = 0
    days_succeeded = 0
    rows_total = 0

    for i in range(n_days):
        day = start_date + timedelta(days=i)
        days_attempted += 1

        try:
            inserted = promote_day_delete_insert(conn, day)
            days_succeeded += 1
            rows_total += inserted
        except Exception as e:
            print(f"[promote_range] ERROR on {day}: {e}")
            continue

    summary = {"days_attempted": days_attempted,
               "days_succeeded": days_succeeded,
               "rows_total": rows_total,
               }
    return summary



if __name__ == "__main__":
    # Choose any day you know has staging data
    test_day = date(2025, 10, 24)

    print("==============================================")
    print("[1] Connecting to DB at:", DB_PATH)
    print("==============================================")

    with get_conn() as conn:

        # ------------------------------------------
        # 1) Raw staging row counts (includes dupes)
        # ------------------------------------------
        print("\n[2] Staging row counts (raw, includes duplicates):")
        print(count_staging_rows_for_day(conn, test_day))

        # ------------------------------------------
        # 2) Distinct-slot coverage (completeness check)
        # ------------------------------------------
        print("\n[3] Distinct-slot coverage (should be complete):")
        cov = distinct_slot_coverage(conn, test_day)
        print(cov)

        # ------------------------------------------
        # 3) Canonical slice preview
        # ------------------------------------------
        print("\n[4] Build canonical slice (last-write-wins):")
        canon = build_canonical_slice_for_day(conn, test_day)
        print(f"Canonical slice rows: {len(canon)}")
        print(canon.head())

        # ------------------------------------------
        # 4) Test promote_day_delete_insert
        # ------------------------------------------
        print("\n[5] Testing promote_day_delete_insert() three times:")

        for run in range(1, 4):
            inserted = promote_day_delete_insert(conn, test_day)

            # Count fact_readings rows for the local-day UTC window
            tz = ZoneInfo("UTC")
            start_utc = datetime(test_day.year, test_day.month, test_day.day,
                                 0, 0, 0, tzinfo=tz)
            end_utc = start_utc + timedelta(days=1)

            cur = conn.cursor()
            cur.execute("""
                SELECT COUNT(*) FROM fact_readings
                WHERE ts_utc >= ? AND ts_utc < ?
            """, (
                start_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
                end_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
            ))
            (fact_rows,) = cur.fetchone()

            print(f"[promote] Run {run}: inserted={inserted}, fact_rows={fact_rows}")

        # ------------------------------------------
        # 5) NEW — Test range promotion
        # ------------------------------------------
        print("\n[6] Testing promote_range_delete_insert():")

        start_range = date(2025, 10, 23)
        end_range = date(2025, 10, 24)

        summary = promote_range_delete_insert(conn, start_range, end_range)
        print("Range summary:", summary)

        print("\n==============================================")
        print("HEALTH CHECK COMPLETE")
        print("==============================================")
