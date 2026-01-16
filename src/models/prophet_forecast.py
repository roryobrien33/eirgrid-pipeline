"""
prophet_forecast.py

Helpers to:
- load historical metric data from the warehouse
- prepare it for Prophet
- fit a Prophet model
- forecast the next day (96 x 15min steps)

Robust mode:
- retry Prophet fit a few times
- if Prophet fails, fall back to deterministic slot-median forecast per metric

IMPORTANT (no leakage):
- When backfilling, pass as_of_day=<D-1> so the model trains ONLY on data available up to that day.
- The forecast produced will be for D (next day after as_of_day).
"""

from __future__ import annotations

from datetime import date, timedelta
import time

import pandas as pd
from pandas.api.types import is_datetime64_any_dtype, is_float_dtype
from prophet import Prophet
import matplotlib.pyplot as plt

from models.fallback_forecast import fallback_slot_median_next_day
from warehouse.readings import get_metric_series, get_latest_complete_local_day


# ---------------------------------------------------------------------
# 1. Load metric history for Prophet
# ---------------------------------------------------------------------
def load_metric_history_for_prophet(
    metric_code: str,
    train_days: int,
    *,
    as_of_day: date | None = None,
) -> pd.DataFrame:
    """
    Load a slice of metric history from fact_readings suitable for Prophet training.

    Parameters
    ----------
    metric_code : str
        One of "wind_actual", "solar_actual", "demand_actual".
    train_days : int
        Number of calendar days to load, inclusive, ending on `as_of_day`.
    as_of_day : date | None
        If None: uses get_latest_complete_local_day()
        If provided: forces the training window to end on this day (prevents leakage).

    Returns
    -------
    pd.DataFrame with at least: ts_utc (tz-aware), value
    """
    if not isinstance(metric_code, str) or not metric_code.strip():
        raise TypeError("metric_code must be a non-empty string.")

    if not isinstance(train_days, int):
        raise TypeError("train_days must be an integer.")
    if train_days <= 0:
        raise ValueError("train_days must be a positive integer (e.g. 7, 30, 60).")

    if as_of_day is None:
        as_of_day = get_latest_complete_local_day()
    if not isinstance(as_of_day, date):
        raise TypeError("as_of_day must be a datetime.date or None.")

    start_day = as_of_day - timedelta(days=train_days - 1)

    df = get_metric_series(metric_code, start_day, as_of_day)

    if df.empty:
        raise ValueError(
            f"No data found for metric_code={metric_code!r} between {start_day} and {as_of_day}. "
            "Check that ETL has promoted data into fact_readings."
        )

    if "ts_utc" in df.columns:
        df = df.sort_values("ts_utc").reset_index(drop=True)

    return df


# ---------------------------------------------------------------------
# 2. Convert warehouse frame → Prophet frame (ds, y)
# ---------------------------------------------------------------------
def to_prophet_frame(df_metric: pd.DataFrame) -> pd.DataFrame:
    """
    Convert fact_readings-style dataframe into Prophet format.

    Input columns:
      - ts_utc : datetime64[ns, UTC] (tz-aware)
      - value  : numeric

    Output:
      - ds : naive datetime64[ns] (interpreted as UTC timestamps)
      - y  : float
    """
    required = {"ts_utc", "value"}
    if not required.issubset(df_metric.columns):
        missing = required - set(df_metric.columns)
        raise ValueError(f"Missing required columns: {missing}")

    df_prophet = df_metric.rename(columns={"ts_utc": "ds", "value": "y"})
    df_prophet = df_prophet[["ds", "y"]]

    # Prophet requires naive datetimes; warehouse ts_utc is tz-aware UTC.
    df_prophet["ds"] = df_prophet["ds"].dt.tz_convert(None)

    df_prophet["y"] = df_prophet["y"].astype(float)
    df_prophet = df_prophet.sort_values("ds").reset_index(drop=True)
    return df_prophet


# ---------------------------------------------------------------------
# 3. Fit Prophet model
# ---------------------------------------------------------------------
def fit_prophet(df_prophet: pd.DataFrame) -> Prophet:
    """
    Fit Prophet on df with columns:
      - ds: datetime64[ns] (naive)
      - y : float
    """
    required = {"ds", "y"}
    if not required.issubset(df_prophet.columns):
        missing = required - set(df_prophet.columns)
        raise ValueError(f"Missing required columns for Prophet: {missing}")

    if not is_datetime64_any_dtype(df_prophet["ds"]):
        raise TypeError("Column 'ds' must be datetime64[ns] for Prophet.")
    if not is_float_dtype(df_prophet["y"]):
        raise TypeError("Column 'y' must be float dtype for Prophet.")

    if len(df_prophet) < 96:
        raise ValueError(f"Not enough rows to train Prophet (got {len(df_prophet)}, need at least 96).")

    model = Prophet(
        daily_seasonality=True,
        weekly_seasonality=True,
        yearly_seasonality=False,
        interval_width=0.9,
    )
    model.fit(df_prophet)
    return model


# ---------------------------------------------------------------------
# 4. Forecast helpers
# ---------------------------------------------------------------------
def _build_next_96_steps_from_training(df_prophet: pd.DataFrame) -> pd.DataFrame:
    """
    Build 96 x 15-min steps starting immediately after last training timestamp.
    """
    last_ds = df_prophet["ds"].max()
    start = last_ds + timedelta(minutes=15)
    future_ds = pd.date_range(start=start, periods=96, freq="15min")
    return pd.DataFrame({"ds": future_ds})


def forecast_next_day_for_metric(
    metric_code: str,
    train_days: int,
    *,
    as_of_day: date | None = None,
) -> pd.DataFrame:
    """
    Prophet-only next-day forecast (96 x 15-min steps).

    If as_of_day is provided, training ends on that day and forecast is for as_of_day+1.
    """
    df_history = load_metric_history_for_prophet(metric_code, train_days, as_of_day=as_of_day)
    df_prophet = to_prophet_frame(df_history)

    model = fit_prophet(df_prophet)
    df_future = _build_next_96_steps_from_training(df_prophet)

    forecast = model.predict(df_future)

    result = forecast[["ds", "yhat", "yhat_lower", "yhat_upper"]].copy()
    result["metric_code"] = metric_code

    for col in ["yhat", "yhat_lower", "yhat_upper"]:
        result[col] = result[col].clip(lower=0.0)

    return result


# ---------------------------------------------------------------------
# 4b. Robust: retry Prophet, else fallback
# ---------------------------------------------------------------------
def forecast_next_day_for_metric_robust(
    metric_code: str,
    train_days: int,
    *,
    as_of_day: date | None = None,
    max_retries: int = 2,
    retry_sleep_seconds: float = 0.5,
) -> pd.DataFrame:
    """
    Attempts Prophet forecast. If it fails after retries, uses fallback_slot_median_next_day.

    Returns columns:
      ds, yhat, yhat_lower, yhat_upper, metric_code, model_name
    """
    df_history = load_metric_history_for_prophet(metric_code, train_days, as_of_day=as_of_day)
    df_prophet = to_prophet_frame(df_history)

    last_err: Exception | None = None

    for attempt in range(max_retries + 1):
        try:
            model = fit_prophet(df_prophet)
            df_future = _build_next_96_steps_from_training(df_prophet)
            forecast = model.predict(df_future)

            out = forecast[["ds", "yhat", "yhat_lower", "yhat_upper"]].copy()
            out["metric_code"] = metric_code
            out["model_name"] = "prophet_v1"

            for col in ["yhat", "yhat_lower", "yhat_upper"]:
                out[col] = out[col].clip(lower=0.0)

            return out

        except Exception as e:
            last_err = e
            if attempt < max_retries:
                time.sleep(retry_sleep_seconds * (attempt + 1))

    # Fallback forecast for the next day after last training timestamp
    forecast_date = (df_prophet["ds"].max() + timedelta(minutes=15)).date()
    fb = fallback_slot_median_next_day(df_prophet, forecast_date).copy()
    fb["metric_code"] = metric_code
    fb["model_name"] = "fallback_slot_median_v1"

    for col in ["yhat", "yhat_lower", "yhat_upper"]:
        fb[col] = fb[col].clip(lower=0.0)

    required_out = {"ds", "yhat", "yhat_lower", "yhat_upper", "metric_code", "model_name"}
    if not required_out.issubset(fb.columns):
        missing = required_out - set(fb.columns)
        raise ValueError(
            f"Fallback forecast did not return required columns: {missing}. "
            f"Original Prophet error was: {last_err!r}"
        )

    return fb


# ---------------------------------------------------------------------
# 5. Multi-metric runner
# ---------------------------------------------------------------------
def forecast_all_metrics_next_day(
    train_days: int = 30,
    *,
    as_of_day: date | None = None,
) -> pd.DataFrame:
    """
    Run a next-day forecast for all core metrics.

    If as_of_day is provided, all metrics are trained ending on that day, and forecast for as_of_day+1.
    """
    metric_codes = ["wind_actual", "solar_actual", "demand_actual"]

    frames: list[pd.DataFrame] = []
    for code in metric_codes:
        frames.append(
            forecast_next_day_for_metric_robust(
                metric_code=code,
                train_days=train_days,
                as_of_day=as_of_day,
                max_retries=2,
            )
        )

    df_all = pd.concat(frames, ignore_index=True)
    df_all = df_all.sort_values(["ds", "metric_code"]).reset_index(drop=True)

    if "model_name" not in df_all.columns:
        df_all["model_name"] = "prophet_v1"

    return df_all


def plot_next_day_forecast_all(train_days: int = 30) -> None:
    df_all = forecast_all_metrics_next_day(train_days=train_days)

    if df_all.empty:
        raise ValueError("Forecast dataframe is empty — cannot produce a plot.")

    df_wind = df_all[df_all["metric_code"] == "wind_actual"]
    df_solar = df_all[df_all["metric_code"] == "solar_actual"]
    df_demand = df_all[df_all["metric_code"] == "demand_actual"]

    fig, ax = plt.subplots(figsize=(12, 6))
    ax.plot(df_wind["ds"], df_wind["yhat"], label="Wind (MW)", linewidth=2)
    ax.plot(df_solar["ds"], df_solar["yhat"], label="Solar (MW)", linewidth=2)
    ax.plot(df_demand["ds"], df_demand["yhat"], label="Demand (MW)", linewidth=2)

    ax.set_title(f"Next-Day Forecast (train_days={train_days})", fontsize=14)
    ax.set_xlabel("Time (UTC)", fontsize=12)
    ax.set_ylabel("Forecasted Power (MW)", fontsize=12)
    ax.grid(True, linestyle="--", alpha=0.4)
    ax.legend()
    fig.autofmt_xdate()
    plt.tight_layout()
    plt.show()


if __name__ == "__main__":
    df_all = forecast_all_metrics_next_day(train_days=30)
    print(df_all.head(12))
    print()
    print("Any negatives in yhat?", (df_all["yhat"] < 0).any())
    print("Any negatives in yhat_lower?", (df_all["yhat_lower"] < 0).any())
    print("Any negatives in yhat_upper?", (df_all["yhat_upper"] < 0).any())
    print()
    print("Rows:", len(df_all))
    print(df_all["metric_code"].value_counts())
    print(df_all["ds"].min(), "→", df_all["ds"].max())
    print()
    if "model_name" in df_all.columns:
        print("Model usage:")
        print(df_all.groupby(["metric_code", "model_name"]).size())
