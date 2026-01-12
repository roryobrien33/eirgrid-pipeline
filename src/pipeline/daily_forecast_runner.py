"""
daily_forecast_runner.py

Orchestration entry point for the DAILY automated job:

1) Run ETL for yesterday (Europe/Dublin local day) -> stage + promote into fact_readings
2) Generate next-day forecasts for all metrics using a sliding training window
3) Store forecasts into fact_forecasts (idempotent: delete-then-insert)
4) Optionally export a CSV snapshot to data/processed/forecasts/
5) Export dashboard-ready Parquet dataset(s) for Power BI (forecast vs actual)

This is the script you schedule via Windows Task Scheduler.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
import argparse
import logging

import pandas as pd

from pipeline.daily_runner import run_daily_pipeline, get_yesterday_local
from warehouse.readings import get_latest_complete_local_day
from models.prophet_forecast import forecast_all_metrics_next_day
from models.store_forecasts import store_forecast_dataframe

# NEW: dashboard parquet exporter
from dashboard.export_dashboard_parquet import export_demand_forecast_vs_actual_parquet


# ---------------------------------------------------------------------
# Logging hygiene: suppress cmdstanpy / prophet spam in CMD/Task Scheduler
# ---------------------------------------------------------------------
logging.getLogger("cmdstanpy").setLevel(logging.CRITICAL)
logging.getLogger("prophet").setLevel(logging.CRITICAL)

# (optional) suppress noisy warnings some users see in batch runs
logging.getLogger("pystan").setLevel(logging.CRITICAL)


# Resolve project root (…/eirgrid-pipeline/)
INIT_PATH = Path(__file__)
PROJECT_ROOT = INIT_PATH.resolve().parents[2]


@dataclass
class DailyForecastResult:
    etl_day: date
    etl_rows_inserted: int
    latest_complete_day: date
    forecast_date: date
    train_days: int
    region_code: str
    forecast_rows: int
    models_used: list[str]
    csv_path: str | None
    dashboard_parquet_path: str | None


def _ensure_forecast_has_model_name(df: pd.DataFrame, default_model_name: str) -> pd.DataFrame:
    """
    Ensure the forecast dataframe includes a model_name column.

    If upstream already adds model_name per-row, keep it.
    If not, stamp the provided default on all rows.
    """
    if "model_name" not in df.columns:
        df = df.copy()
        df["model_name"] = default_model_name
    else:
        # Normalize blanks
        df = df.copy()
        df["model_name"] = df["model_name"].astype(str)
        df.loc[df["model_name"].str.strip() == "", "model_name"] = default_model_name
    return df


def _infer_forecast_date_from_df(df: pd.DataFrame) -> date:
    """
    Infer the intended forecast_date from the dataframe's ds column.

    Assumes df['ds'] is datetime-like and that all rows fall on the same day.
    """
    if "ds" not in df.columns:
        raise ValueError("Forecast dataframe is missing required column: 'ds'")

    ds = pd.to_datetime(df["ds"], errors="coerce")
    if ds.isna().any():
        raise ValueError("Forecast dataframe has non-parsable values in 'ds'.")

    min_day = ds.min().date()
    max_day = ds.max().date()
    if min_day != max_day:
        raise ValueError(
            f"Forecast 'ds' spans multiple days ({min_day} → {max_day}). "
            "Expected a single next-day horizon."
        )
    return min_day


def run_daily_forecast_pipeline(
    train_days: int = 60,
    save_csv: bool = True,
    default_model_name: str = "prophet_v1",
    region_code: str = "ALL",
    export_dashboard: bool = True,
) -> DailyForecastResult:
    """
    Run the end-to-end daily job:
      - ETL yesterday into fact_readings
      - Forecast next day
      - Store into fact_forecasts
      - Optionally export CSV
      - Optionally export dashboard Parquet (forecast vs actual)

    Returns a structured summary for logging/CLI output.
    """

    # -----------------------------
    # 1) Validate inputs
    # -----------------------------
    if not isinstance(train_days, int) or train_days <= 0:
        raise ValueError("train_days must be a positive integer.")
    if not isinstance(default_model_name, str) or not default_model_name.strip():
        raise ValueError("default_model_name must be a non-empty string.")
    if not isinstance(region_code, str) or not region_code.strip():
        raise ValueError("region_code must be a non-empty string.")

    default_model_name = default_model_name.strip()
    region_code = region_code.strip()

    # -----------------------------
    # 2) Run ETL for yesterday
    # -----------------------------
    etl_day = get_yesterday_local()
    print(f"[daily_forecast] Running ETL for: {etl_day} ...")

    etl_rows = run_daily_pipeline(etl_day)
    print(f"[daily_forecast] ETL complete: inserted {etl_rows} rows into fact_readings.")

    # -----------------------------
    # 3) Determine forecast date (next day after latest complete local day)
    # -----------------------------
    latest_complete = get_latest_complete_local_day()
    expected_forecast_date = latest_complete + timedelta(days=1)

    print(f"[daily_forecast] Latest complete local day: {latest_complete}")
    print(f"[daily_forecast] Forecast date (next day):  {expected_forecast_date}")
    print(f"[daily_forecast] Training window:           {train_days} days")

    # -----------------------------
    # 4) Generate forecast dataframe
    # -----------------------------
    df_forecast = forecast_all_metrics_next_day(train_days=train_days)

    if df_forecast is None or not isinstance(df_forecast, pd.DataFrame):
        raise ValueError("Forecast function did not return a DataFrame.")
    if df_forecast.empty:
        raise ValueError("Forecast returned an empty DataFrame.")
    if "metric_code" not in df_forecast.columns:
        raise ValueError("Forecast dataframe is missing required column: 'metric_code'")

    # Ensure model_name exists (robust forecaster usually sets per-row; otherwise stamp default)
    df_forecast = _ensure_forecast_has_model_name(df_forecast, default_model_name=default_model_name)

    # Infer forecast_date from df itself (source of truth) and reconcile with expected
    inferred_forecast_date = _infer_forecast_date_from_df(df_forecast)
    if inferred_forecast_date != expected_forecast_date:
        print(
            "[daily_forecast] WARNING: forecast_date mismatch.\n"
            f"  expected (latest_complete+1): {expected_forecast_date}\n"
            f"  inferred from df['ds']:       {inferred_forecast_date}\n"
            "  Using inferred forecast date for storage/export."
        )
    forecast_date = inferred_forecast_date

    forecast_rows = len(df_forecast)
    metrics = sorted(df_forecast["metric_code"].astype(str).unique().tolist())
    models_used = sorted(df_forecast["model_name"].astype(str).unique().tolist())
    print(f"[daily_forecast] Forecast generated: {forecast_rows} rows across {metrics}")
    print(f"[daily_forecast] Models used: {models_used}")

    # -----------------------------
    # 5) Store forecasts into DB (idempotent)
    # -----------------------------
    # If forecasts contain multiple model_name values (e.g., prophet + fallback),
    # store them per-model so delete-then-insert remains correct.
    storage_summaries: list[dict] = []
    for model_name, df_model in df_forecast.groupby("model_name", dropna=False):
        model_name_str = str(model_name)

        summary = store_forecast_dataframe(
            df=df_model,
            forecast_date=forecast_date,
            train_days=train_days,
            model_name=model_name_str,
            region_code=region_code,
        )
        storage_summaries.append(summary)

    print(f"[daily_forecast] Stored forecasts → fact_forecasts (n={len(storage_summaries)} batches).")
    for s in storage_summaries:
        print(f"  - {s}")

    # -----------------------------
    # 6) Optional CSV export
    # -----------------------------
    csv_path: str | None = None
    if save_csv:
        forecasts_dir = PROJECT_ROOT / "data" / "processed" / "forecasts"
        forecasts_dir.mkdir(parents=True, exist_ok=True)

        filename = f"forecast_{forecast_date.isoformat()}_train{train_days}d.csv"
        out_path = forecasts_dir / filename

        df_forecast.to_csv(out_path, index=False)
        csv_path = str(out_path)
        print(f"[daily_forecast] Saved CSV → {csv_path}")

    # -----------------------------
    # 7) Dashboard Parquet export (Power BI source)
    # -----------------------------
    dashboard_parquet_path: str | None = None
    if export_dashboard:
        try:
            # NOTE: exporter currently creates demand_forecast_vs_actual.parquet
            export_summary = export_demand_forecast_vs_actual_parquet()
            dashboard_parquet_path = str(export_summary.get("out_path")) if export_summary else None
            if export_summary:
                print(
                    "[daily_forecast] Exported dashboard Parquet → "
                    f"{export_summary.get('out_path')} (rows={export_summary.get('rows')})"
                )
            else:
                print("[daily_forecast] Exported dashboard Parquet (no summary returned).")
        except Exception as e:
            # Keep the pipeline successful even if dashboard export fails.
            # This avoids breaking the automated daily job over a reporting artifact.
            print(f"[daily_forecast] WARNING: dashboard Parquet export failed: {e}")

    # -----------------------------
    # 8) Return summary object
    # -----------------------------
    return DailyForecastResult(
        etl_day=etl_day,
        etl_rows_inserted=int(etl_rows),
        latest_complete_day=latest_complete,
        forecast_date=forecast_date,
        train_days=train_days,
        region_code=region_code,
        forecast_rows=forecast_rows,
        models_used=models_used,
        csv_path=csv_path,
        dashboard_parquet_path=dashboard_parquet_path,
    )


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="Run daily ETL + next-day forecasting + forecast storage (+ optional dashboard export)."
    )
    parser.add_argument(
        "--train-days",
        type=int,
        default=60,
        help="How many past days to use for training (default = 60).",
    )
    parser.add_argument(
        "--no-save",
        action="store_true",
        help="Run pipeline but do NOT write CSV output.",
    )
    parser.add_argument(
        "--default-model-name",
        type=str,
        default="prophet_v1",
        help="Default model identifier used if forecast output lacks model_name.",
    )
    parser.add_argument(
        "--region-code",
        type=str,
        default="ALL",
        help="Region code stored in fact_forecasts (default = ALL).",
    )
    parser.add_argument(
        "--no-dashboard-export",
        action="store_true",
        help="Do NOT export dashboard Parquet output (Power BI source).",
    )

    args = parser.parse_args(argv)

    result = run_daily_forecast_pipeline(
        train_days=args.train_days,
        save_csv=not args.no_save,
        default_model_name=args.default_model_name,
        region_code=args.region_code,
        export_dashboard=not args.no_dashboard_export,
    )

    print("\n[daily_forecast] DONE")
    print(result)


if __name__ == "__main__":
    import sys

    try:
        main(sys.argv[1:])
    except Exception as e:
        print(f"\nERROR: daily_forecast_runner failed: {e}")
        raise
