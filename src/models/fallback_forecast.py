import pandas as pd
from datetime import timedelta

def fallback_slot_median_next_day(df_hist: pd.DataFrame, forecast_date) -> pd.DataFrame:
    """
    Deterministic forecast:
    For each time-of-day (HH:MM), use the median historical value across df_hist.
    Expects df_hist columns: ['ds', 'y'] where ds is datetime-like.
    Returns Prophet-like frame: ['ds','yhat','yhat_lower','yhat_upper'] for next day at 15-min cadence.
    """
    d = df_hist.copy()
    d["ds"] = pd.to_datetime(d["ds"])
    d["slot"] = d["ds"].dt.strftime("%H:%M")

    # robust central tendency per slot
    slot_med = d.groupby("slot")["y"].median()

    # build next day's 15-min grid based on observed cadence
    start = pd.Timestamp(forecast_date)
    times = pd.date_range(start=start, periods=96, freq="15min")

    out = pd.DataFrame({"ds": times})
    out["slot"] = out["ds"].dt.strftime("%H:%M")
    out["yhat"] = out["slot"].map(slot_med).fillna(d["y"].median())
    out["yhat"] = out["yhat"].clip(lower=0)

    # conservative intervals (you can tighten later)
    mad = (d["y"] - d["y"].median()).abs().median()
    out["yhat_lower"] = (out["yhat"] - 2 * mad).clip(lower=0)
    out["yhat_upper"] = out["yhat"] + 2 * mad

    return out[["ds", "yhat", "yhat_lower", "yhat_upper"]]
