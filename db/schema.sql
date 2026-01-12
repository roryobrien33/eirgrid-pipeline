
CREATE TABLE IF NOT EXISTS dim_metric(
    metric_id       INTEGER PRIMARY KEY,        -- rowid alias
    metric_code     TEXT    NOT NULL UNIQUE,    -- e.g. 'wind_actual'
    unit            TEXT    NOT NULL            -- e.g. 'MW'
);

CREATE TABLE IF NOT EXISTS dim_region(
    region_id       INTEGER PRIMARY KEY,
    region_code     TEXT    NOT NULL UNIQUE     -- e.g. 'ALL'
);

CREATE TABLE IF NOT EXISTS stg_readings(
    ts_utc          TEXT    NOT NULL,           -- ISO-8601 UTC: 'YYYY-MM-DDTHH:MM:SSZ'
    metric_code     TEXT    NOT NULL,           -- matches dim_metric.metric_code
    region_code     TEXT    NOT NULL,           -- matches dim_region.region_code
    value           REAL,                       -- NULL allowed (partial days, gaps)
    source          TEXT    NOT NULL,           -- e.g. 'smartgriddashboard_api'
    ingested_at     TEXT    NOT NULL            -- ISO-8601 UTC timestamp set by app
    -- Note:  NOTE: no PK/UNIQUE here; staging is truth-preserving, append-only.
);

CREATE TABLE IF NOT EXISTS fact_readings (
    ts_utc      TEXT    NOT NULL,                 -- ISO-8601 UTC
    metric_id   INTEGER NOT NULL REFERENCES dim_metric(metric_id),
    region_id   INTEGER NOT NULL REFERENCES dim_region(region_id),
    value       REAL,                              -- keep nullable; policy is "don’t overwrite non-NULL with NULL"
    source      TEXT,                              -- propagate source / lineage
    ingested_at TEXT    NOT NULL,                  -- set by app on upsert
    UNIQUE (ts_utc, metric_id, region_id)          -- idempotent key
);

CREATE TABLE IF NOT EXISTS fact_forecasts (
    forecast_date  TEXT    NOT NULL,    -- the day being predicted (e.g. '2025-12-09')
    ts_utc         TEXT    NOT NULL,    -- timestamp of prediction slot in UTC
    metric_code    TEXT    NOT NULL,    -- wind_actual, solar_actual, demand_actual
    region_code    TEXT    NOT NULL,    -- 'ALL' for now
    train_days     INTEGER NOT NULL,    -- training window size (e.g. 60)
    model_name     TEXT    NOT NULL,    -- e.g. 'prophet'
    yhat           REAL    NOT NULL,
    yhat_lower     REAL    NOT NULL,
    yhat_upper     REAL    NOT NULL,
    generated_utc  TEXT    NOT NULL,    -- when the forecast was produced
    PRIMARY KEY (
        forecast_date,
        ts_utc,
        metric_code,
        region_code,
        model_name,
        train_days
    )
);


-- Query performance helpers
CREATE INDEX IF NOT EXISTS ix_fact_ts         ON fact_readings (ts_utc);
CREATE INDEX IF NOT EXISTS ix_fact_metric_ts  ON fact_readings (metric_id, ts_utc);
