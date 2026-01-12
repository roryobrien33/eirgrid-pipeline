"""
backfill_range.py

Backfill utility for a sliding window ending yesterday (Europe/Dublin local day).

Modes:
A) ETL-only (default):
   - Runs run_daily_pipeline(day) for each day in the window -> populates fact_readings

B) ETL + Forecasts (--with-forecasts):
   - After ETL, generates day-ahead forecasts for each "as_of_day" in the window:
       as_of_day = D
       forecast_date = D + 1
     Training data MUST end at as_of_day (to avoid leakage).

Notes:
- Forecast storage uses store_forecast_dataframe (idempotent delete-then-insert).
- If forecasting outputs multiple model_name values (prophet + fallback), we store per-model batch.
"""

from __future__ import annotations

from datetime import date, timedelta
import argparse

from pipeline.daily_runner import run_daily_pipeline, get_yesterday_local
from warehouse.readings import get_latest_complete_local_day

from models.prophet_forecast import forecast_all_metrics_next_day
from models.store_forecasts import store_forecast_dataframe


def date_range_generator(start_date: date, end_date: date) -> list[date]:
    """Return a list of dates from start_date to end_date inclusive."""
    dates: list[date] = []
    next_day = start_date
    while next_day <= end_date:
        dates.append(next_day)
        next_day += timedelta(days=1)
    return dates


def backfill_range(days: int) -> dict:
    """
    Run the daily pipeline for a sliding window of days ending at yesterday.

    Example:
      days = 5, yesterday = 2025-12-08
      → runs for 2025-12-04, 05, 06, 07, 08

    Returns a summary dict with:
      - days_attempted
      - days_succeeded
      - rows_by_day   (date -> rows_inserted)
      - rows_total
    """
    if not isinstance(days, int):
        raise TypeError("days must be an integer.")
    if days <= 0:
        raise ValueError("days must be a positive integer.")

    end_date = get_yesterday_local()
    start_date = end_date - timedelta(days=days - 1)
    dates = date_range_generator(start_date, end_date)

    days_attempted = 0
    days_succeeded = 0
    rows_total = 0
    rows_by_day: dict[date, int] = {}

    for day in dates:
        days_attempted += 1
        try:
            rows = run_daily_pipeline(day)
            if rows is None:
                rows = 0

            rows_by_day[day] = int(rows)
            rows_total += int(rows)
            days_succeeded += 1

            print(f"[backfill] {day} → inserted {rows} rows")
        except Exception as e:
            print(f"[backfill] ERROR on {day}: {e}")
            continue

    return {
        "days_attempted": days_attempted,
        "days_succeeded": days_succeeded,
        "rows_by_day": rows_by_day,
        "rows_total": rows_total,
        "start_date": start_date,
        "end_date": end_date,
    }


def backfill_forecasts_for_range(
    start_date: date,
    end_date: date,
    forecast_train_days: int,
    region_code: str = "ALL",
    default_model_name: str = "prophet_v1",
) -> dict:
    """
    Generate day-ahead forecasts for each as_of_day in [start_date, end_date].

    For each as_of_day D:
      - Train using data ending at D (inclusive)
      - Forecast next day (D+1) (96 x 15-min steps per metric)
      - Store into fact_forecasts (idempotent)

    Returns a summary dict.
    """
    if not isinstance(forecast_train_days, int) or forecast_train_days <= 0:
        raise ValueError("forecast_train_days must be a positive integer.")
    if not isinstance(region_code, str) or not region_code.strip():
        raise ValueError("region_code must be a non-empty string.")
    if not isinstance(default_model_name, str) or not default_model_name.strip():
        raise ValueError("default_model_name must be a non-empty string.")

    region_code = region_code.strip()
    default_model_name = default_model_name.strip()

    days = date_range_generator(start_date, end_date)

    forecasts_attempted = 0
    forecasts_succeeded = 0
    stored_batches_total = 0
    stored_rows_total = 0
    summaries: list[dict] = []
    failures: list[dict] = []

    # Optional sanity: confirm latest_complete_local_day covers end_date
    latest_complete = get_latest_complete_local_day()
    if latest_complete < end_date:
        print(
            "[backfill_forecasts] WARNING: latest_complete_local_day is behind requested range.\n"
            f"  latest_complete_local_day: {latest_complete}\n"
            f"  requested end_date:        {end_date}\n"
            "  Forecast backfill can still run, but training windows may be shorter than expected."
        )

    for as_of_day in days:
        forecasts_attempted += 1
        try:
            print(f"[backfill_forecasts] as_of_day={as_of_day} → forecasting next day...")

            # IMPORTANT: This call MUST train only up to as_of_day (no leakage).
            # Your models.prophet_forecast.forecast_all_metrics_next_day must support as_of_day.
            df_forecast = forecast_all_metrics_next_day(
                train_days=forecast_train_days,
                as_of_day=as_of_day,
            )

            if df_forecast is None or df_forecast.empty:
                raise ValueError("forecast_all_metrics_next_day returned empty dataframe.")

            # Ensure model_name exists
            if "model_name" not in df_forecast.columns:
                df_forecast = df_forecast.copy()
                df_forecast["model_name"] = default_model_name

            # Infer forecast_date from df['ds'] (should all be the same next-day)
            forecast_date = df_forecast["ds"].min().date()

            # Store per model_name batch (supports prophet + fallback mix)
            batches_for_day = 0
            rows_for_day = 0

            for model_name, df_model in df_forecast.groupby("model_name", dropna=False):
                model_name_str = str(model_name)

                s = store_forecast_dataframe(
                    df=df_model,
                    forecast_date=forecast_date,
                    train_days=forecast_train_days,
                    model_name=model_name_str,
                    region_code=region_code,
                )
                summaries.append(s)
                batches_for_day += 1
                rows_for_day += int(s.get("rows_deleted_then_inserted", 0))

            stored_batches_total += batches_for_day
            stored_rows_total += rows_for_day
            forecasts_succeeded += 1

            print(
                f"[backfill_forecasts] OK as_of_day={as_of_day} "
                f"(forecast_date={forecast_date}) batches={batches_for_day} rows={rows_for_day}"
            )

        except Exception as e:
            msg = str(e)
            failures.append({"as_of_day": as_of_day.isoformat(), "error": msg})
            print(f"[backfill_forecasts] ERROR as_of_day={as_of_day}: {msg}")
            continue

    return {
        "forecasts_attempted": forecasts_attempted,
        "forecasts_succeeded": forecasts_succeeded,
        "stored_batches_total": stored_batches_total,
        "stored_rows_total": stored_rows_total,
        "region_code": region_code,
        "forecast_train_days": forecast_train_days,
        "summaries": summaries,
        "failures": failures,
    }


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Backfill ETL (and optionally forecasts) over a sliding window.")
    parser.add_argument(
        "--days",
        type=int,
        default=30,
        help="How many days to backfill ending yesterday (default = 30).",
    )
    parser.add_argument(
        "--with-forecasts",
        action="store_true",
        help="Also generate day-ahead forecasts for each as_of_day in the backfill window.",
    )
    parser.add_argument(
        "--forecast-train-days",
        type=int,
        default=60,
        help="Training window length used for each backfilled forecast (default = 60).",
    )
    parser.add_argument(
        "--region-code",
        type=str,
        default="ALL",
        help="Region code to store in fact_forecasts (default = ALL).",
    )
    parser.add_argument(
        "--default-model-name",
        type=str,
        default="prophet_v1",
        help="Default model_name if the forecast output lacks model_name (default = prophet_v1).",
    )

    args = parser.parse_args(argv)

    print(f"[backfill] Starting ETL backfill for last {args.days} days (ending yesterday)...")
    etl_summary = backfill_range(days=args.days)
    print("\n[backfill] ETL summary:")
    print(etl_summary)

    if args.with_forecasts:
        start_date = etl_summary["start_date"]
        end_date = etl_summary["end_date"]

        print(
            "\n[backfill_forecasts] Starting forecast backfill...\n"
            f"  as_of_day range:      {start_date} → {end_date}\n"
            f"  forecast_train_days:  {args.forecast_train_days}\n"
            f"  region_code:          {args.region_code}\n"
        )

        forecast_summary = backfill_forecasts_for_range(
            start_date=start_date,
            end_date=end_date,
            forecast_train_days=args.forecast_train_days,
            region_code=args.region_code,
            default_model_name=args.default_model_name,
        )

        print("\n[backfill_forecasts] Forecast summary:")
        print(forecast_summary)


if __name__ == "__main__":
    main()
