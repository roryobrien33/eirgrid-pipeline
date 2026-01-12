-- ============================================================
-- views.sql
-- Analytical views for dashboards & BI tools
-- ============================================================

-- Drop views if they already exist (idempotent)
DROP VIEW IF EXISTS vw_readings_flat;
DROP VIEW IF EXISTS vw_forecasts;
DROP VIEW IF EXISTS vw_forecast_vs_actual;

-- ------------------------------------------------------------
-- 1) Flattened readings (joins dims)
-- ------------------------------------------------------------
CREATE VIEW vw_readings_flat AS
SELECT
    fr.ts_utc,
    dm.metric_code,
    dr.region_code,
    fr.value,
    dm.unit,
    fr.source,
    fr.ingested_at
FROM fact_readings fr
JOIN dim_metric dm ON dm.metric_id = fr.metric_id
JOIN dim_region dr ON dr.region_id = fr.region_id;

-- ------------------------------------------------------------
-- 2) Forecasts (already flat, just normalize timestamps)
-- ------------------------------------------------------------
CREATE VIEW vw_forecasts AS
SELECT
    forecast_date,
    ts_utc,
    metric_code,
    region_code,
    train_days,
    model_name,
    yhat,
    yhat_lower,
    yhat_upper,
    generated_utc
FROM fact_forecasts;

-- ------------------------------------------------------------
-- 3) Forecast vs Actual (DEMAND only, first pass)
-- ------------------------------------------------------------
CREATE VIEW vw_forecast_vs_actual AS
SELECT
    f.forecast_date,
    f.ts_utc,
    f.metric_code,
    f.region_code,
    f.model_name,
    f.train_days,

    f.yhat                         AS forecast_mw,
    r.value                        AS actual_mw,
    (r.value - f.yhat)             AS error_mw,
    ABS(r.value - f.yhat)          AS abs_error_mw

FROM fact_forecasts f
LEFT JOIN vw_readings_flat r
  ON r.ts_utc = f.ts_utc
 AND r.metric_code = f.metric_code
 AND r.region_code = f.region_code

WHERE f.metric_code = 'demand_actual';

-- ------------------------------------------------------------
-- 4) Dashboard default run (pick ONE run to avoid duplicates)
--    Update these literals when you want to change the default.
-- ------------------------------------------------------------
DROP VIEW IF EXISTS vw_forecast_vs_actual_default;

CREATE VIEW vw_forecast_vs_actual_default AS
SELECT
    f.forecast_date,
    f.ts_utc,
    f.metric_code,
    f.region_code,
    f.model_name,
    f.train_days,
    f.yhat                         AS forecast_mw,
    r.value                        AS actual_mw,
    (r.value - f.yhat)             AS error_mw,
    ABS(r.value - f.yhat)          AS abs_error_mw
FROM fact_forecasts f
LEFT JOIN vw_readings_flat r
  ON r.ts_utc = f.ts_utc
 AND r.metric_code = f.metric_code
 AND r.region_code = f.region_code
WHERE f.metric_code = 'demand_actual'
  AND f.model_name  = 'prophet_v1'
  AND f.train_days  = 60;

-- ------------------------------------------------------------
-- 5) All-metrics forecast vs actual (future-proofing)
--    Still returns multiple rows per ts if multiple runs exist.
-- ------------------------------------------------------------
DROP VIEW IF EXISTS vw_forecast_vs_actual_all;

CREATE VIEW vw_forecast_vs_actual_all AS
SELECT
    f.forecast_date,
    f.ts_utc,
    f.metric_code,
    f.region_code,
    f.model_name,
    f.train_days,
    f.yhat                         AS forecast_mw,
    r.value                        AS actual_mw,
    (r.value - f.yhat)             AS error_mw,
    ABS(r.value - f.yhat)          AS abs_error_mw
FROM fact_forecasts f
LEFT JOIN vw_readings_flat r
  ON r.ts_utc = f.ts_utc
 AND r.metric_code = f.metric_code
 AND r.region_code = f.region_code;
