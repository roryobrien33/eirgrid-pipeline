from pathlib import Path
from datetime import timedelta
import pandas as pd

from models.prophet_forecast import forecast_all_metrics_next_day
from warehouse.readings import get_latest_complete_local_day

import argparse


# Paths
INIT_PATH = Path(__file__)
PROJECT_ROOT = INIT_PATH.resolve().parents[2]


def run_next_day_forecasts(train_days: int = 30, save_csv: bool = True) -> pd.DataFrame:
    """
    Run next-day forecasts for ALL metrics (wind, solar, demand).

    This function:
      1. Determines the latest COMPLETE local day in the DB
      2. Generates a next-day forecast for that date
      3. Validates forecast structure
      4. Saves a CSV (optional)
      5. Returns the forecast DataFrame

    Parameters
    ----------
    train_days : int
        Number of historical calendar days to use for Prophet training
        Example: 30 → train on last 30 complete days
    save_csv : bool
        Whether to write forecast output to data/processed/forecasts/

    Returns
    -------
    pd.DataFrame
        A DataFrame with 288 rows (96 × 3 metrics) containing:
        ds, yhat, yhat_lower, yhat_upper, metric_code
    """

    # ------------------------------------------------------------------
    # 1. Validate inputs
    # ------------------------------------------------------------------
    if train_days <= 0:
        raise ValueError("train_days must be a positive integer.")

    # ------------------------------------------------------------------
    # 2. Determine the forecast target date
    # ------------------------------------------------------------------
    latest_day = get_latest_complete_local_day()
    forecast_for = latest_day + timedelta(days=1)

    # ------------------------------------------------------------------
    # 3. Run forecasting for all metrics
    # ------------------------------------------------------------------
    df_forecast = forecast_all_metrics_next_day(train_days=train_days)

    if df_forecast.empty:
        raise ValueError("Forecast function returned an empty DataFrame.")

    # ------------------------------------------------------------------
    # 4. Validate expected columns
    # ------------------------------------------------------------------
    expected_cols = {"ds", "yhat", "yhat_lower", "yhat_upper", "metric_code"}
    actual_cols = set(df_forecast.columns)

    if not expected_cols.issubset(actual_cols):
        missing = expected_cols - actual_cols
        raise ValueError(f"Forecast output missing columns: {missing}")

    # ------------------------------------------------------------------
    # 5. Print summary for user visibility
    # ------------------------------------------------------------------
    metrics = df_forecast["metric_code"].unique()
    row_count = len(df_forecast)

    print(f"\nLatest complete local day: {latest_day}")
    print(f"Forecast generated for:   {forecast_for}")
    print(f"Rows returned:            {row_count}")
    print(f"Metrics included:         {list(metrics)}\n")

    # ------------------------------------------------------------------
    # 6. Save CSV output (optional)
    # ------------------------------------------------------------------
    if save_csv:
        forecasts_dir = PROJECT_ROOT / "data" / "processed" / "forecasts"
        forecasts_dir.mkdir(parents=True, exist_ok=True)

        filename = f"forecast_{forecast_for.isoformat()}_train{train_days}d.csv"
        out_path = forecasts_dir / filename

        df_forecast.to_csv(out_path, index=False)

        print(f"Saved forecast CSV → {out_path}\n")

    # ------------------------------------------------------------------
    # 7. Return DataFrame to caller (Python, notebook, scheduler, etc.)
    # ------------------------------------------------------------------
    return df_forecast


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="Run next-day forecasts for all metrics (wind, solar, demand)."
    )

    # Optional argument, default = 30
    parser.add_argument(
        "--train-days",
        type=int,
        default=30,
        help="How many past days to use for training (default = 30)."
    )

    # Flag: when present, DO NOT save CSV
    parser.add_argument(
        "--no-save",
        action="store_true",
        help="Run forecast but do NOT write CSV output."
    )

    # Parse the command-line args
    args = parser.parse_args(argv)

    # Convert flag → function argument
    save_csv = not args.no_save

    # Execute forecasting
    df = run_next_day_forecasts(
        train_days=args.train_days,
        save_csv=save_csv,
    )

    # Show a quick preview
    print("\nPreview:")
    print(df.head())

if __name__ == "__main__":
    import sys
    main(sys.argv[1:])

