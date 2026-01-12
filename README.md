EirGrid Renewable Energy, Demand \& Forecasting Pipeline



Automated ingestion, warehousing, forecasting, and analytics pipeline for EirGrid Smart Grid Dashboard data (wind, solar, system demand).



This project demonstrates a complete, production-style data engineering and applied forecasting workflow:

robust HTTP ingestion, retry/backoff, timezone-safe parsing, normalized data warehousing,

idempotent daily runs, rolling demand forecasting, historical forecast backfills,

and BI-ready dataset export for Power BI dashboards.



Features



API ingestion with retry/backoff

⦁ Handles 5xx and timeout events using retry logic

⦁ Fails fast on malformed or invalid 4xx responses



Timezone \& DST-aware parsing

⦁ API timestamps are Europe/Dublin local time

⦁ Converted to canonical UTC ISO-8601 format (YYYY-MM-DDTHH:MM:SSZ)

⦁ Correctly handles DST transitions (92 / 96 / 100 intervals per day)



Normalized warehouse schema

⦁ Dimension tables: dim\_metric, dim\_region

⦁ stg\_readings: append-only raw truth

⦁ fact\_readings: canonical, deduplicated measurements

⦁ fact\_forecasts: persisted forecast outputs



Idempotent fact promotion

⦁ A day is promoted only if all expected 15-minute slots are present

⦁ Existing fact rows for the UTC window are deleted and reinserted atomically

⦁ Last-write-wins semantics ensure deterministic results



Forecasting pipeline

⦁ Rolling next-day demand forecasts

⦁ One forecast per forecast\_date (no data leakage)

⦁ Prophet-based statistical model with fallback safety model

⦁ Forecast uncertainty intervals (lower / upper bounds)

⦁ Negative-value clipping to enforce physical realism



Historical backfills

⦁ Backfill of historical actuals

⦁ Backfill of historical forecasts for model evaluation

⦁ Forecasts are trained only on data available at the time



Analytics \& BI integration

⦁ Parquet export optimized for Power BI

⦁ 15-minute resolution actual vs forecast curves

⦁ Error metrics (RMSE, MAPE)

⦁ Interactive dashboard slicing by forecast date



Architecture Overview (ASCII)



┌────────────────────────────┐

│ Smart Grid Dashboard API │

│ wind / solar / demand │

└───────────────┬────────────┘

│ JSON

▼

┌──────────────────────────┐

│ fetch\_data.py │

│ - retry / backoff │

│ - parse JSON │

│ - local → UTC conversion │

│ - tidy dataframe │

└───────────────┬──────────┘

│ stage

▼

┌──────────────────────────┐

│ stg\_readings (SQLite) │

│ append-only raw truth │

└───────────────┬──────────┘

│ promote

▼

┌──────────────────────────┐

│ fact\_readings │

│ canonical measurements │

└───────────────┬──────────┘

│

├──────────────┐

▼ ▼

┌─────────────────────┐ ┌──────────────────────┐

│ Forecast models │ │ Historical backfills │

│ (Prophet + fallback)│ │ (rolling forecasts) │

└──────────┬──────────┘ └──────────┬───────────┘

▼ ▼

┌──────────────────────────┐

│ fact\_forecasts │

│ persisted predictions │

└───────────────┬──────────┘

▼

┌──────────────────────────┐

│ Parquet export │

│ Power BI dashboards │

└──────────────────────────┘



Repository Structure



eirgrid-pipeline/

├─ data/

│ └─ processed/

│ └─ dashboard/

│ └─ demand\_forecast\_vs\_actual.parquet

│

├─ db/

│ └─ eirgrid.db (SQLite database, gitignored)

│

├─ notebooks/

│ └─ exploratory analysis and validation

│

├─ src/

│ ├─ ingest/

│ │ ├─ init\_db.py (create database + tables)

│ │ ├─ seed\_dims.py (load dimension tables)

│ │ ├─ fetch\_data.py (API ingestion)

│ │ ├─ stage.py (stg\_readings inserts)

│ │ └─ promote.py (canonical promotion)

│ │

│ ├─ models/

│ │ ├─ prophet\_forecast.py (Prophet forecasting logic)

│ │ ├─ fallback\_forecast.py (safety fallback model)

│ │ ├─ run\_forecasts.py (forecast orchestration)

│ │ └─ store\_forecasts.py (fact\_forecasts persistence)

│ │

│ ├─ pipeline/

│ │ ├─ daily\_runner.py (ETL only)

│ │ ├─ daily\_forecast\_runner.py (ETL + next-day forecast)

│ │ └─ backfill\_range.py (historical backfills)

│ │

│ ├─ dashboard/

│ │ ├─ export\_dashboard\_parquet.py

│ │ └─ Power BI dashboards (.pbix)

│ │

│ └─ warehouse/

│ └─ readings.py (warehouse access helpers)

│

├─ tests/

├─ tmp/

├─ .env.example

├─ requirements.txt

├─ run\_daily\_forecast.bat

└─ README.txt



Setup



4.1 Environment



Python 3.11 recommended.



Install dependencies:

pip install -r requirements.txt



4.2 Environment Variables



Copy and edit:

.env.example → .env



Example:

EIRGRID\_BASE\_URL=https://www.smartgriddashboard.com/api/chart/



USER\_AGENT=EirGridPipeline/1.0 (contact: Rory O'Brien)

LOCAL\_TZ=Europe/Dublin



4.3 Database Initialization



Run once from project root:

python -m ingest.init\_db

python -m ingest.seed\_dims



Data Flow Explained



5.1 Fetch \& Stage (fetch\_data.py)



⦁ Builds multi-area API requests (wind, solar, demand)

⦁ Parses JSON defensively

⦁ Converts Dublin local timestamps to UTC

⦁ Produces tidy rows

⦁ Inserts into stg\_readings



5.2 Canonical Promotion (promote.py)



Promotion only occurs if:

⦁ All expected 15-minute intervals exist

⦁ Each metric-region pair is complete



Canonicalisation steps:

⦁ Load staging rows for the UTC window

⦁ Sort by ingestion timestamp

⦁ Apply last-write-wins logic

⦁ Map dimension codes to IDs

⦁ Insert clean rows into fact\_readings



Forecasting Pipeline



⦁ One forecast generated per forecast\_date

⦁ Model is trained only on historical data available at that date

⦁ No leakage of future actuals into training

⦁ Forecast horizon is next-day (96 × 15-minute points)

⦁ Forecasts persisted to fact\_forecasts



Negative forecast values are clipped to 0.0 MW to respect physical constraints.



Daily Operation



Run ETL + next-day forecast:

python -m pipeline.daily\_forecast\_runner



This is the production entry point and is safe for Windows Task Scheduler.



Historical Backfills



Backfill actuals only:

python -m pipeline.backfill\_range --days 30



Backfill actuals and historical forecasts:

python -m pipeline.backfill\_range --days 30 --with-forecasts --forecast-train-days 60



Each historical forecast is a true rolling forecast generated without access to future data.



Dashboard Export



Generate the Power BI dataset:

python -m dashboard.export\_dashboard\_parquet



Output:

data/processed/dashboard/demand\_forecast\_vs\_actual.parquet



This file is the single source of truth for BI.



Dashboard Capabilities



⦁ Actual vs forecast demand curves

⦁ 15-minute resolution

⦁ Forecast uncertainty intervals

⦁ RMSE and MAPE metrics

⦁ Forecast-date slicing

⦁ Model and training window context



Roadmap



⦁ Multi-region forecasting

⦁ Additional renewable forecasts

⦁ Model comparison dashboards

⦁ DuckDB or Postgres backend

⦁ Cloud storage and query layer

⦁ Orchestration with Prefect or Airflow



Author



Rory O’Brien

GitHub: https://github.com/roryobrien33



