from datetime import date, timedelta, datetime
import pandas as pd
from zoneinfo import ZoneInfo
from ingest.promote import get_conn, get_dim_maps


def get_metric_series(metric_code:str, start_date: date, end_date: date):
    if not isinstance(metric_code, str):
        raise TypeError("metric_code must be a string")
    if not isinstance(start_date, date):
        raise TypeError("start_date must be a date")
    if not isinstance(end_date, date):
        raise TypeError("end_date must be a date")

    start_utc = f"{start_date.isoformat()}T00:00:00Z"
    end_utc = f"{(end_date + timedelta(days=1)).isoformat()}T00:00:00Z"

    sql = """
           SELECT ts_utc, value, metric_id, region_id
           FROM fact_readings
           WHERE metric_id = ? 
           AND ts_utc >= ? 
           AND ts_utc < ?
           ORDER BY ts_utc;
       """

    with get_conn() as conn:
        metric_map, region_map = get_dim_maps(conn)
        if metric_code not in metric_map:
            raise ValueError(f"Unknown metric code: {metric_code!r}")

        metric_id = metric_map[metric_code]

        df = pd.read_sql(sql, conn,params=(metric_id, start_utc, end_utc))
        df["ts_utc"] = pd.to_datetime(df["ts_utc"], utc=True, errors="coerce")
        df = df.sort_values("ts_utc").reset_index(drop=True)

        return df

def get_all_metrics_wide(start_date: date, end_date: date) -> pd.DataFrame:

    get_wind = get_metric_series("wind_actual", start_date, end_date)
    get_solar = get_metric_series("solar_actual", start_date, end_date)
    get_demand = get_metric_series("demand_actual", start_date, end_date)

    df_wind = get_wind.rename(columns={"value": "wind_actual"}).drop(columns=["metric_id","region_id"], axis=1)
    df_solar = get_solar.rename(columns={"value": "solar_actual"}).drop(columns=["metric_id","region_id"], axis=1)
    df_demand = get_demand.rename(columns={"value": "demand_actual"}).drop(columns=["metric_id","region_id"], axis=1)

    join_df =df_wind.merge(df_solar, on=["ts_utc"]).merge(df_demand, on=["ts_utc"])

    return join_df

def get_latest_complete_local_day(tz: ZoneInfo = ZoneInfo("Europe/Dublin")) -> date:
    """
    Look at fact_readings, find the maximum ts_utc, interpret it as a UTC
    timestamp, convert to the given local timezone, and return the local
    calendar date.

    This is used to find the most recent complete day that has been
    promoted into fact_readings.
    """
    sql = "SELECT MAX(ts_utc) FROM fact_readings;"

    with get_conn() as conn:
        row = conn.execute(sql).fetchone()

    max_ts_str = row[0]

    if max_ts_str is None:
        raise RuntimeError(
            "No rows found in fact_readings; "
            "run the daily pipeline before calling get_latest_complete_local_day()."
        )

    # ts_utc is stored as TEXT like "2025-10-24T22:45:00Z"
    # datetime.fromisoformat understands "+00:00" better than "Z"
    ts_utc = datetime.fromisoformat(max_ts_str.replace("Z", "+00:00"))

    # Convert to local timezone and take the date component
    ts_local = ts_utc.astimezone(tz)
    latest_day_local = ts_local.date()

    return latest_day_local


if __name__ == "__main__":
    latest = get_latest_complete_local_day()
    print("Latest complete local day:", latest)
    print()
    print(get_all_metrics_wide(latest, latest).head())



