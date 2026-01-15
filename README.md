EirGrid Renewable Energy, Demand \& Forecasting Pipeline



Automated ingestion, warehousing, forecasting, and analytics pipeline for

EirGrid Smart Grid Dashboard data (wind, solar, system demand).



This project demonstrates a complete, production-style data engineering and

applied forecasting workflow: robust HTTP ingestion, retry/backoff,

timezone-safe parsing, normalized data warehousing, idempotent daily runs,

rolling demand forecasting, historical forecast backfills, and BI-ready

dataset export for Power BI dashboards.



────────────────────────────────────────────────────────────────────────



Quick Start (Recommended)



This project is designed to run as a fully containerised, one-command pipeline.

No local Python installation is required.



Prerequisites

⦁ Docker Desktop (Windows / macOS / Linux)

⦁ Docker Compose v2+



Run the full pipeline (ETL + next-day forecast)



Windows (PowerShell):

.\\run.ps1



macOS / Linux:

./run.sh



This single command will:

⦁ Build the Docker image

⦁ Initialise the SQLite warehouse (idempotent)

⦁ Ingest the latest complete day of EirGrid data

⦁ Generate next-day demand forecasts

⦁ Persist outputs to:

&nbsp; - db/eirgrid.db

&nbsp; - data/processed/forecasts/\*.csv

&nbsp; - data/processed/dashboard/\*.parquet



Custom training window



Windows:

.\\run.ps1 -TrainDays 30



macOS / Linux:

./run.sh --train-days 30



Docker is the recommended and supported execution method.



────────────────────────────────────────────────────────────────────────



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



────────────────────────────────────────────────────────────────────────



Architecture Overview (ASCII)



Smart Grid Dashboard API

&nbsp; └─ wind / solar / system demand (JSON)



&nbsp;       ↓



Ingestion Layer

&nbsp; └─ fetch\_data.py

&nbsp;    - HTTP retry / backoff

&nbsp;    - JSON validation

&nbsp;    - Europe/Dublin → UTC conversion

&nbsp;    - 15-minute tidy records



&nbsp;       ↓



Staging Layer (SQLite)

&nbsp; └─ stg\_readings

&nbsp;    - Append-only raw truth

&nbsp;    - All ingested observations



&nbsp;       ↓



Canonical Warehouse

&nbsp; └─ fact\_readings

&nbsp;    - Deduplicated measurements

&nbsp;    - Idempotent daily promotion

&nbsp;    - Last-write-wins semantics



&nbsp;       ↓



Forecasting Layer

&nbsp; └─ Prophet-based demand forecasts

&nbsp;    - Rolling next-day forecasts

&nbsp;    - Leakage-safe training window

&nbsp;    - Fallback safety model

&nbsp;    - Stored in fact\_forecasts



&nbsp;       ↓



Analytics \& BI Export

&nbsp; └─ Parquet datasets

&nbsp;    - Actual vs forecast curves

&nbsp;    - Error metrics (RMSE, MAPE)

&nbsp;    - Power BI–ready format



────────────────────────────────────────────────────────────────────────



Repository Structure



eirgrid-pipeline/

├─ data/

│  └─ processed/ (gitignored)

│     ├─ forecasts/

│     └─ dashboard/

│

├─ db/

│  └─ eirgrid.db (SQLite database, gitignored)

│

├─ notebooks/

│  └─ exploratory analysis and validation

│

├─ src/

│  ├─ ingest/

│  │  ├─ init\_db.py

│  │  ├─ seed\_dims.py

│  │  ├─ fetch\_data.py

│  │  ├─ stage.py

│  │  └─ promote.py

│  │

│  ├─ models/

│  │  ├─ prophet\_forecast.py

│  │  ├─ fallback\_forecast.py

│  │  ├─ run\_forecasts.py

│  │  └─ store\_forecasts.py

│  │

│  ├─ pipeline/

│  │  ├─ daily\_runner.py

│  │  ├─ daily\_forecast\_runner.py

│  │  └─ backfill\_range.py

│  │

│  ├─ dashboard/

│  │  └─ export\_dashboard\_parquet.py

│  │

│  └─ warehouse/

│     └─ readings.py

│

├─ docker/

│  └─ entrypoint.sh

├─ docker-compose.yml

├─ Dockerfile

├─ run.ps1

├─ run.sh

├─ .env.example

├─ requirements.txt

└─ README.md



────────────────────────────────────────────────────────────────────────



Optional: Local Python Development (Advanced)



This section is intended for contributors or users who wish to run the pipeline

without Docker. Docker execution is recommended.



Environment



Python 3.11 recommended.



Install dependencies:

pip install -r requirements.txt



Environment Variables



Copy and edit:

.env.example → .env



Example:

EIRGRID\_BASE\_URL=https://www.smartgriddashboard.com/api/chart/

USER\_AGENT=EirGridPipeline/1.0 (educational / research use)

LOCAL\_TZ=Europe/Dublin



Database Initialization



Run once:

python -m ingest.init\_db

python -m ingest.seed\_dims



────────────────────────────────────────────────────────────────────────



Daily Operation



Production Execution (Recommended)



Windows:

.\\run.ps1



macOS / Linux:

./run.sh



Local Execution (Optional)



python -m pipeline.daily\_forecast\_runner



────────────────────────────────────────────────────────────────────────



Historical Backfills



Backfill actuals only:

python -m pipeline.backfill\_range --days 30



Backfill actuals and historical forecasts:

python -m pipeline.backfill\_range --days 30 --with-forecasts --forecast-train-days 60



Each historical forecast is a true rolling forecast generated without access to

future data.



────────────────────────────────────────────────────────────────────────



Dashboard Export



Generate the Power BI dataset:

python -m dashboard.export\_dashboard\_parquet



Output:

data/processed/dashboard/demand\_forecast\_vs\_actual.parquet



This file is the single source of truth for BI.



────────────────────────────────────────────────────────────────────────



Forecasting Scope \& Design Decisions



Although the pipeline ingests, stores, and canonicalizes wind, solar, and system

demand data, the forecasting and dashboard layer intentionally focuses on system

demand only.



This reflects modelling realism and portfolio scope rather than technical

limitation.



(remaining sections unchanged from original: demand rationale, renewables

discussion, design intent)



────────────────────────────────────────────────────────────────────────



Roadmap



⦁ Multi-region forecasting

⦁ Additional renewable forecasts

⦁ Model comparison dashboards

⦁ DuckDB or Postgres backend

⦁ Cloud storage and query layer

⦁ Orchestration with Prefect or Airflow



────────────────────────────────────────────────────────────────────────

Author



Rory O’Brien  

GitHub: https://github.com/roryobrien33



