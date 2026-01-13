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

│ └─ processed (gitignored)

│  └─ dashboard/ (gitignored)

│  └─ demand\_forecast\_vs\_actual.parquet (gitignored)

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



Canonicalization steps:

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



Forecasting Scope \& Design Decisions



Although the pipeline ingests, stores, and canonicalizes wind, solar, and system demand data, the forecasting and dashboard layer in this project intentionally focuses on system demand only.



This decision is deliberate and reflects practical modelling, data-quality, and portfolio-scoping considerations rather than a technical limitation.



Why Demand Was Prioritised



System demand is:



⦁ A continuous signal with relatively stable temporal structure

⦁ Less sensitive to short-term exogenous volatility than renewables

⦁ Well-suited to univariate statistical forecasting methods (e.g. Prophet)

⦁ Directly interpretable for model evaluation using standard error metrics (RMSE, MAPE)



This makes demand an appropriate first forecasting target for demonstrating:



⦁ Rolling, leakage-safe forecast generation

⦁ Historical backfilled forecasts for evaluation

⦁ Forecast storage, comparison, and BI integration

⦁ End-to-end production-style ML orchestration



Why Wind and Solar Were Not Forecasted (Yet)



Wind and solar generation exhibit characteristics that require additional modelling complexity beyond the scope of this iteration:



⦁ Strong dependence on exogenous variables (weather, irradiance, wind speed)

⦁ Structural zero-generation periods (e.g. solar at night)

⦁ Higher short-term volatility and regime changes

⦁ Greater sensitivity to capacity changes and curtailment



While simple statistical forecasts can be produced, doing so without incorporating weather features or capacity context risks producing misleading results.



Rather than include weaker or unrealistic renewable forecasts, the project intentionally limits forecasting to demand while still fully ingesting and storing renewable actuals for future extension.



Design Intent



This project is structured so that:



⦁ Renewable forecasting can be added cleanly without refactoring the pipeline

⦁ Additional models (e.g. weather-driven, hybrid, or ML-based) can coexist in fact\_forecasts

⦁ Dashboards can be extended to compare demand forecasts with renewable penetration or net demand



In other words, wind and solar are first-class data citizens in the warehouse, even though they are not yet forecasted in this iteration.



This mirrors real-world production systems, where pipelines often support more data than is immediately modelled, allowing forecasting capability to evolve incrementally without architectural change.



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

