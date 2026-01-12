import pandas as pd
from datetime import datetime, timezone, date

from ingest.promote import get_conn


def store_forecast_dataframe(
    df: pd.DataFrame,
    forecast_date: date,
    train_days: int,
    model_name: str = "prophet_v1",
    region_code: str = "ALL",
) -> dict:
    """
    Store a long-format next-day forecast dataframe into fact_forecasts.

    Required df columns:
      ds, yhat, yhat_lower, yhat_upper, metric_code

    Optional df columns:
      model_name (recommended), region_code

    Writes:
      forecast_date, ts_utc, metric_code, region_code, train_days,
      yhat, yhat_lower, yhat_upper, generated_utc, model_name

    Idempotency:
      For each distinct (forecast_date, region_code, model_name, train_days) present in df,
      delete existing rows then insert the new batch.
    """

    # ----------------------------
    # 1) Validate inputs
    # ----------------------------
    required = {"ds", "yhat", "yhat_lower", "yhat_upper", "metric_code"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Forecast dataframe missing required columns: {missing}")

    if not isinstance(forecast_date, date):
        raise TypeError(f"forecast_date must be datetime.date, got {type(forecast_date)}")

    if not isinstance(train_days, int) or train_days <= 0:
        raise ValueError("train_days must be a positive integer.")

    if df.empty:
        raise ValueError("df is empty; nothing to store.")

    # Domain safety: MW cannot be negative
    for col in ("yhat", "yhat_lower", "yhat_upper"):
        vals = pd.to_numeric(df[col], errors="raise")
        if (vals < 0).any():
            raise ValueError(f"{col} contains negative values; expected all >= 0.")

    # ----------------------------
    # 2) Normalise ds -> UTC ISO string ts_utc
    # ----------------------------
    df_local = df.copy()

    ds = pd.to_datetime(df_local["ds"], errors="raise")
    # Prophet often returns naive datetimes; treat naive as UTC.
    if getattr(ds.dt, "tz", None) is None:
        ds_utc = ds.dt.tz_localize("UTC")
    else:
        ds_utc = ds.dt.tz_convert("UTC")

    df_local["ts_utc"] = ds_utc.dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    forecast_date_str = forecast_date.isoformat()
    generated_utc_str = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    # ----------------------------
    # 3) Ensure model_name / region_code columns exist
    # ----------------------------
    if "model_name" not in df_local.columns:
        df_local["model_name"] = model_name
    else:
        df_local["model_name"] = df_local["model_name"].astype(str)
        df_local.loc[df_local["model_name"].str.strip() == "", "model_name"] = model_name

    if "region_code" not in df_local.columns:
        df_local["region_code"] = region_code
    else:
        df_local["region_code"] = df_local["region_code"].astype(str)
        df_local.loc[df_local["region_code"].str.strip() == "", "region_code"] = region_code

    # ----------------------------
    # 4) Build the frame we will write (schema-aligned)
    # ----------------------------
    df_to_write = pd.DataFrame(
        {
            "forecast_date": forecast_date_str,
            "ts_utc": df_local["ts_utc"].astype(str),
            "metric_code": df_local["metric_code"].astype(str),
            "region_code": df_local["region_code"].astype(str),
            "train_days": int(train_days),
            "yhat": df_local["yhat"].astype(float),
            "yhat_lower": df_local["yhat_lower"].astype(float),
            "yhat_upper": df_local["yhat_upper"].astype(float),
            "generated_utc": generated_utc_str,
            "model_name": df_local["model_name"].astype(str),
        }
    )

    # ----------------------------
    # 5) Delete + Insert (idempotent per present group)
    # ----------------------------
    delete_sql = """
        DELETE FROM fact_forecasts
        WHERE forecast_date = ?
          AND region_code = ?
          AND model_name = ?
          AND train_days = ?
    """

    insert_sql = """
        INSERT INTO fact_forecasts (
            forecast_date,
            ts_utc,
            metric_code,
            region_code,
            train_days,
            yhat,
            yhat_lower,
            yhat_upper,
            generated_utc,
            model_name
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    # distinct delete groups based on what we are about to write
    delete_groups = (
        df_to_write[["region_code", "model_name"]]
        .drop_duplicates()
        .itertuples(index=False, name=None)
    )

    # rows to insert
    rows = df_to_write[
        [
            "forecast_date",
            "ts_utc",
            "metric_code",
            "region_code",
            "train_days",
            "yhat",
            "yhat_lower",
            "yhat_upper",
            "generated_utc",
            "model_name",
        ]
    ].itertuples(index=False, name=None)

    with get_conn() as conn:
        cur = conn.cursor()

        # Delete per (forecast_date, region_code, model_name, train_days)
        for (rc, mn) in delete_groups:
            cur.execute(delete_sql, (forecast_date_str, rc, mn, int(train_days)))

        # Insert
        cur.executemany(insert_sql, rows)
        conn.commit()

    # Summarise model usage stored
    model_counts = (
        df_to_write.groupby(["metric_code", "model_name"])
        .size()
        .sort_values(ascending=False)
        .to_dict()
    )

    return {
        "forecast_date": forecast_date_str,
        "train_days": int(train_days),
        "rows_deleted_then_inserted": int(len(df_to_write)),
        "generated_utc": generated_utc_str,
        "model_usage": model_counts,
    }


if __name__ == "__main__":
    from models.prophet_forecast import forecast_all_metrics_next_day

    print("Running smoke test for store_forecast_dataframe()...")

    train_days = 5
    df_forecast = forecast_all_metrics_next_day(train_days=train_days)

    forecast_date = pd.to_datetime(df_forecast["ds"]).min().date()

    summary = store_forecast_dataframe(
        df=df_forecast,
        forecast_date=forecast_date,
        train_days=train_days,
        model_name="prophet_v1_test",
        region_code="ALL",
    )

    print("Storage summary:")
    print(summary)
