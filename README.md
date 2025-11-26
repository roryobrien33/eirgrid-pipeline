
EirGrid Renewable Energy & Demand Pipeline

Automated daily ingestion, staging, canonicalization, and fact-table promotion for EirGrid’s Smart Grid Dashboard data (wind, solar, demand).

This project demonstrates a complete, production-style data engineering workflow:
robust HTTP ingestion, retry/backoff, timezone-safe parsing, staging → fact promotion,
idempotent daily runs, and a command-line daily runner suitable for cron/scheduled tasks.

1. Features

API ingestion with retry/backoff
- Handles 5xx/timeout events using linear backoff
- Fails fast on malformed 4xx responses

Timezone & DST-aware parsing
- API timestamps are Europe/Dublin local time
- Converted to UTC ISO-8601 (2025-10-24T00:15:00Z)
- Handles DST transitions correctly (92/96/100 slots)

Normalized schema
- dim_metric, dim_region
- stg_readings (append-only raw truth)
- fact_readings (clean, deduplicated, last-write-wins)

Idempotent fact promotion
- A day is only promoted if it is complete (all expected 15-minute slots)
- Promotion deletes existing fact rows for the UTC window and inserts canonical rows

Daily pipeline runner
- Defaults to “yesterday (Europe/Dublin)”
- Can run for any date using --day YYYY-MM-DD
- Safe to schedule in cron / Windows Task Scheduler

2. Architecture Diagram (ASCII)
             ┌────────────────────────────┐
             │  Smart Grid Dashboard API  │
             │  wind / solar / demand     │
             └───────────────┬────────────┘
                             │ JSON
                             ▼
                ┌──────────────────────────┐
                │ fetch_data.py            │
                │ - retry/backoff          │
                │ - parse JSON → raw DF    │
                │ - local → UTC conversion │
                │ - tidy dataframe         │
                └───────────────┬──────────┘
                                │ stage_readings()
                                ▼
                ┌──────────────────────────┐
                │ stg_readings (SQLite)    │
                │ append-only, raw truth   │
                └───────────────┬──────────┘
                                │ promote_day_delete_insert()
                                ▼
                ┌──────────────────────────┐
                │ fact_readings (SQLite)   │
                │ - canonical daily slice  │
                │ - latest-ingest wins     │
                └───────────────┬──────────┘
                                │
                                ▼
                  ┌────────────────────────┐
                  │ downstream analytics    │
                  │ dashboards / ML / BI    │
                  └────────────────────────┘

3. Repository Structure
eirgrid-pipeline/
├─ db/
│   ├─ schema.sql          # all DDL for dimensions, staging, facts
│   └─ eirgrid.db          # SQLite database (ignored by git)
│
├─ src/
│   ├─ ingest/
│   │    ├─ __init__.py
│   │    ├─ init_db.py           # create db + tables
│   │    ├─ seed_dims.py         # load metric/region dimensions
│   │    ├─ fetch_data.py        # HTTP → tidy → stage
│   │    ├─ stage.py             # insert tidy rows into stg_readings
│   │    └─ promote.py           # canonical daily promotion to facts
│   │
│   └─ pipeline/
│        ├─ __init__.py
│        └─ daily_runner.py      # CLI, cron-friendly pipeline runner
│
├─ notebooks/
│   └─ 00_api_probe.ipynb        # exploratory API probing
│
├─ data/
│   ├─ raw/
│   └─ processed/
│
├─ .env.example
├─ requirements.txt
└─ README.md

4. Setup
4.1 Environment
Python 3.11 recommended.
pip install -r requirements.txt

4.2 Environment Variables
Copy and edit:
cp .env.example .env

Example .env:
EIRGRID_BASE_URL=https://www.smartgriddashboard.com/api/chart/
USER_AGENT=EirGridPipeline/1.0 (contact: Rory O'Brien; github.com/roryobrien33)
LOCAL_TZ=Europe/Dublin

4.3 Initializing the Database
Run from the project root:
python -m ingest.init_db
python -m ingest.seed_dims

5. Data Flow Explained
5.1 Fetch & Stage (fetch_data.py)
- Builds query parameters (areas = wind, solar, demand)
- Sends a single multi-area HTTP request per day
- Parses JSON rows safely
- Converts Dublin local timestamps → UTC
- Produces tidy columns:
  ts_utc, metric, value, unit, region, ts_local
- Inserts them into stg_readings

5.2 Canonical Daily Promotion (promote.py)
Promotion only occurs if:
- All expected 15-minute slots exist
- Each metric-region pair is complete

Canonicalisation steps:
- Load all staging rows for the UTC window
- Sort by ingested_at
- Last-write-wins per (ts_utc, metric_code, region_code)
- Map codes → dimension IDs
- Insert clean rows into fact_readings

6. Daily Pipeline Runner (pipeline/daily_runner.py)
CLI Usage

Run for yesterday (default):
python -m pipeline.daily_runner

Run for a specific day:
python -m pipeline.daily_runner --day 2025-10-24

Example output:
Running daily pipeline for 2025-10-24...
[debug] calling with params: {...}
Staged 288 rows.
[promote] 2025-10-24 → inserted 288 canonical rows into fact_readings
Pipeline complete.

Cron Example (Linux)
0 7 * * * /usr/bin/python /path/to/project/src/pipeline/daily_runner.py

Windows Task Scheduler
Action:
Program: python.exe
Arguments: path\to\pipeline\daily_runner.py

7. Timezone & DST Notes
- API timestamps are Europe/Dublin
- DST is handled explicitly:
  nonexistent='shift_forward' (spring forward)
  ambiguous='NaT' (fall back)
- Canonical UTC timestamps stored in fact tables ensure
  deterministic ML and BI queries.

8. Roadmap
- CLI support for multi-day ranges
- Export to Parquet or DuckDB
- Streamlit dashboard (real-time wind/solar/demand)
- Prefect/Airflow orchestration
- Automated S3 upload + Athena/Presto integration

9. Author
Rory O’Brien
GitHub: https://github.com/roryobrien33
