# === Imports ===
import os
import time
import json
import requests
import pandas as pd
from datetime import datetime, date, timedelta
from zoneinfo import ZoneInfo
from pathlib import Path
from ingest.stage import stage_readings
from dotenv import load_dotenv  # <-- new import

# === Load environment variables ===
load_dotenv()  # Reads .env file so os.getenv() can access its values

# === Configuration / Constants ===

# Environment variables
BASE_URL = os.getenv("EIRGRID_BASE_URL")
USER_AGENT = os.getenv("USER_AGENT")
LOCAL_TZ = os.getenv("LOCAL_TZ", "Europe/Dublin")  # fallback default

# Request defaults
DEFAULT_REGION = "ALL"
DEFAULT_CHART_TYPE = "default"
DEFAULT_DATERANGE = "day"
REQUEST_TIMEOUT_S = 15
MAX_RETRIES = 3
BACKOFF_S = [1, 2, 4]   # len must be >= MAX_RETRIES

# Headers
DEFAULT_HEADERS = {"User-Agent": USER_AGENT}

# Date / time formats
API_DATE_FMT = "%d-%b-%Y"            # e.g., 24-Oct-2025
EFFECTIVE_TIME_FMT = "%d-%b-%Y %H:%M:%S"   # e.g., 24-Oct-2025 00:15:00

# Area → metric mappings
AREA_TO_METRIC = {
    "windactual":   "wind_actual",
    "solaractual":  "solar_actual",
    "demandactual": "demand_actual",
}
FIELDNAME_TO_METRIC = {
    "WIND_ACTUAL":   "wind_actual",
    "SOLAR_ACTUAL":  "solar_actual",
    "DEMAND_ACTUAL": "demand_actual",
    "SYSTEM_DEMAND": "demand_actual",
}

# Default areas to request
DEFAULT_AREAS = ["windactual", "solaractual", "demandactual"]

# === Sanity / guardrail checks ===
if not BASE_URL:
    raise RuntimeError("EIRGRID_BASE_URL is missing. Set it in your .env file.")
if not USER_AGENT:
    raise RuntimeError("USER_AGENT is missing. Set it in your .env file.")
if len(BACKOFF_S) < MAX_RETRIES:
    raise RuntimeError("BACKOFF_S must have at least MAX_RETRIES entries.")

# === Optional one-time sanity prints ===
# print("BASE_URL:", BASE_URL)
# print("USER_AGENT:", USER_AGENT)
# print("LOCAL_TZ:", LOCAL_TZ)
# print("DEFAULT_HEADERS:", DEFAULT_HEADERS)
# print("DEFAULT_AREAS:", DEFAULT_AREAS)

def make_params(day_local, areas, region="ALL", chart_type="default", date_range="day"):
    if not isinstance(day_local, date):
        raise ValueError(f"Expected datetime.date, got {type(day_local)} instead.")

    if not isinstance(areas, list) or not areas:
        raise ValueError("Incorrect area list.")

    for area in areas:
        if area not in AREA_TO_METRIC.keys():
            raise ValueError(f"Unknown area '{area}'. Must be one of {list(AREA_TO_METRIC.keys())}.")

    formatted_date = day_local.strftime(API_DATE_FMT)

    areas_str = ",".join(areas)  # <-- join list into a single string

    return {
        "region": region,
        "chartType": chart_type,
        "dateRange": date_range,
        "dateFrom": formatted_date,
        "dateTo": formatted_date,
        "areas": areas_str
    }

def request_with_retry(params):
    # --- Preflight validation (fixed isinstance usage) ---
    if not isinstance(params, dict):
        raise ValueError("params must be a dict")

    if "areas" not in params:
        raise ValueError("Missing 'areas' in params")

    # If areas is still a list, join it into the comma-separated string the API expects
    if isinstance(params["areas"], list):
        params["areas"] = ",".join(params["areas"])
    elif not isinstance(params["areas"], str):
        raise ValueError("'areas' must be a comma-separated string or a list of strings")

    print(f"[debug] calling with params: {params}")  # (ok for now while learning)

    for attempt in range(MAX_RETRIES):
        response = requests.get(
            BASE_URL,
            params=params,
            headers=DEFAULT_HEADERS,
            timeout=REQUEST_TIMEOUT_S,
        )

        if response.status_code == 200:
            # Return the full Response so later steps can read .json(), .url, etc.
            return response

        if 500 <= response.status_code <= 599:
            wait = BACKOFF_S[attempt]
            print(f"[debug] server {response.status_code}; retrying in {wait}s...")
            time.sleep(wait)
            continue

        # 4xx or other unexpected codes → fail fast with context
        raise RuntimeError(
            f"HTTP {response.status_code} (won't retry). "
            f"URL={response.url} BodyHead={response.text[:200]!r}"
        )

    # If we ever exit the loop without returning, raise explicitly
    raise RuntimeError("Exhausted retries without success.")

def parse_json_to_raw_df(response):
    # ---- Input validation ----
    if not hasattr(response, "status_code") or not hasattr(response, "json"):
        raise ValueError("Expected a response-like object with .status_code and .json().")

    if response.status_code != 200:
        url = getattr(response, "url", "<no url>")
        snippet = getattr(response, "text", "")[:200]
        raise RuntimeError(f"Expected 200, got {response.status_code}. URL={url} BodyHead={snippet!r}")

    # ---- Parse JSON safely ----
    try:
        data = response.json()
    except Exception as e:
        snippet = response.text[:200]
        url = getattr(response, "url", "<no url>")
        raise ValueError(f"Invalid JSON. URL={url} BodyHead={snippet!r} Error={e}")

    # ---- Shape checks ----
    if not isinstance(data, dict):
        raise ValueError(f"Expected a dict, not type {type(data)}")

    if "Rows" not in data:
        raise ValueError(f"Missing 'Rows' key. Top-level keys: {list(data.keys())}")

    if not isinstance(data["Rows"], list):
        raise ValueError(f"'Rows' must be a list, not type: {type(data['Rows'])}")

    # Empty day → return well-shaped empty frame
    if len(data["Rows"]) == 0:
        return pd.DataFrame(columns=["Value", "Region", "EffectiveTime", "FieldName"])

    # ---- Build raw DF ----
    df_raw = pd.DataFrame(data["Rows"])

    # ---- Required columns present ----
    required = {"Value", "Region", "EffectiveTime", "FieldName"}
    missing = required - set(df_raw.columns)
    if missing:
        raise ValueError(f"Missing columns: {sorted(missing)}; got={df_raw.columns.tolist()}")

    # ---- Gentle dtype nudge (still raw) ----
    df_raw["Value"] = pd.to_numeric(df_raw["Value"], errors="coerce")

    return df_raw

def tidy_raw_df(df_raw, local_tz=LOCAL_TZ):
    df_tidy = df_raw.rename(columns=
                            {"Value":"value",
                             "Region":"region",
                             "EffectiveTime":"ts_local_str",
                             "FieldName":"metric"})

    print("DEBUG unique FieldName/metric values:", sorted(df_tidy["metric"].unique()))

    unexpected = set(df_tidy["metric"]) - set(FIELDNAME_TO_METRIC.keys())

    if unexpected:
        raise ValueError(f"Unexpected metric: {unexpected}")

    df_tidy["metric"] = df_tidy["metric"].replace(FIELDNAME_TO_METRIC)

    df_tidy["unit"] = "MW"

    df_tidy["ts_local_naive"] = pd.to_datetime(df_tidy["ts_local_str"]
                   , format=EFFECTIVE_TIME_FMT
                   , errors="raise")
    df_tidy["ts_local"] = (df_tidy["ts_local_naive"]
                           .dt.tz_localize(ZoneInfo(local_tz)
                            ,nonexistent= "shift_forward"
                            , ambiguous= "NaT"))
    df_tidy["ts_utc"] = df_tidy["ts_local"].dt.tz_convert("UTC")

    df_tidy = (
        df_tidy[["ts_utc", "metric", "value", "unit", "region", "ts_local"]]
        .sort_values("ts_utc")
        .reset_index(drop=True)
    )

    df_tidy = df_tidy.drop(columns=["ts_local_str", "ts_local_naive"], errors="ignore")

    print("cadence:\n", df_tidy["ts_utc"].diff().value_counts().head())
    print("dupes:", df_tidy.duplicated(subset=["ts_utc", "metric"]).sum())
    print("nulls (value):", df_tidy["value"].isna().sum())
    print("dtypes:\n", df_tidy.dtypes)

    return df_tidy

def fetch_one_day(day_local, areas, source="smartgriddashboard_api"):
    # 1) validate inputs
    if not isinstance(day_local, date):
        raise ValueError(f"Expected datetime.date for day_local, not {type(day_local)}")

    if not isinstance(areas, list) or not areas or not all(isinstance(a, str) for a in areas):
        raise ValueError("areas must be a non-empty list of strings")

    unexpected = set(areas) - set(AREA_TO_METRIC.keys())
    if unexpected:
        raise ValueError(f"Unexpected areas: {sorted(unexpected)}; allowed={sorted(AREA_TO_METRIC.keys())}")

    # 2) build params
    params = make_params(
        day_local,
        areas,
        region=DEFAULT_REGION,
        chart_type=DEFAULT_CHART_TYPE,
        date_range=DEFAULT_DATERANGE,
    )

    # 3) fetch → 4) parse
    resp = request_with_retry(params)
    raw = parse_json_to_raw_df(resp)
    if len(raw) == 0:
        print(f"[{day_local}] areas={areas} → raw=0 → staged=0 (empty day)")
        return 0

    # 5) tidy
    tidy = tidy_raw_df(raw)

    # 6) stage
    inserted = stage_readings(tidy, source=source)

    # 7) log & return
    print(f"[{day_local}] areas={areas} → raw={len(raw)} tidy={len(tidy)} staged={inserted}")
    return inserted

from datetime import date, timedelta
import time

def fetch_range(start_date, end_date, areas=DEFAULT_AREAS):
    # --- validate inputs ---
    if not isinstance(start_date, date) or not isinstance(end_date, date):
        raise ValueError(f"start_date/end_date must be datetime.date (got {type(start_date)}, {type(end_date)})")
    if start_date > end_date:
        raise ValueError(f"start_date must be <= end_date (got {start_date} > {end_date})")
    if not isinstance(areas, list) or not areas or not all(isinstance(a, str) for a in areas):
        raise ValueError("areas must be a non-empty list of strings")
    unexpected = set(areas) - set(AREA_TO_METRIC.keys())
    if unexpected:
        raise ValueError(f"Unexpected areas: {sorted(unexpected)}; allowed={sorted(AREA_TO_METRIC.keys())}")

    # --- loop days ---
    n_days = (end_date - start_date).days + 1
    days_attempted = 0
    days_succeeded = 0
    rows_total = 0

    for i in range(n_days):
        d = start_date + timedelta(days=i)
        days_attempted += 1

        inserted = fetch_one_day(d, areas)  # <- capture return value
        rows_total += inserted
        if inserted > 0:
            days_succeeded += 1

        time.sleep(0.7)  # polite pacing; adjust if needed

    # --- summary ---
    print(f"[range] {start_date} → {end_date} | days={days_attempted} ok={days_succeeded} rows={rows_total}")
    return {"days": days_attempted, "ok": days_succeeded, "rows": rows_total}



if __name__ == "__main__":
    # 1) Build parameters for a test day
    params = make_params(date(2025, 10, 24), ["windactual", "solaractual"])
    print("Params:", params)

    # 2) Make the API request with retry/backoff
    resp = request_with_retry(params)
    print("HTTP response:", resp)

    # 3) Parse JSON → raw dataframe
    raw = parse_json_to_raw_df(resp)
    print(f"[OK] Raw rows: {len(raw)}")
    print(raw.head(3))
    print()

    # 4) Tidy + timezone standardisation
    tidy = tidy_raw_df(raw)
    print(f"[OK] Tidy rows: {len(tidy)}")
    print(tidy.head())
    print()

    # 5) Stage into SQLite
    inserted = stage_readings(tidy)
    print(f"[OK] Inserted {inserted} rows into stg_readings")
    print()

    # 6) Quick DB sanity checks
    PROJECT_ROOT = Path(__file__).resolve().parents[2]
    DB_PATH = PROJECT_ROOT / "db" / "eirgrid.db"

    import sqlite3
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("PRAGMA foreign_keys = ON;")

        total = conn.execute("SELECT COUNT(*) FROM stg_readings;").fetchone()[0]
        print("stg_readings total rows:", total)

        rows = conn.execute(
            "SELECT ts_utc, metric_code, region_code, value, source, ingested_at "
            "FROM stg_readings "
            "ORDER BY ts_utc, metric_code "
            "LIMIT 5;"
        ).fetchall()

        print("Sample rows:")
        for r in rows:
            print(" ", r)

    print("")
    count = fetch_one_day(date(2025, 10, 24), ["windactual", "solaractual"])
    print("Inserted (one day):", count)
    print("")
    summary = fetch_range(date(2025, 10, 23), date(2025, 10, 24), ["windactual", "solaractual"])
    print(summary)
    print("TEST COMPLETE")