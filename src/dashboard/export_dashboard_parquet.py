"""
export_dashboard_parquet.py

Exports the SQLite analytics view `vw_forecast_vs_actual_all`
to a Power BI–friendly Parquet file.

This view already contains:
  - forecast_date
  - ts_utc (15-min UTC timestamp)
  - metric_code
  - region_code
  - model_name
  - train_days
  - forecast_mw
  - actual_mw
  - error_mw
  - abs_error_mw

No joins, no schema guessing, no duplication of logic.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from ingest.promote import get_conn


# Resolve project root (.../eirgrid-pipeline/)
INIT_PATH = Path(__file__)
PROJECT_ROOT = INIT_PATH.resolve().parents[2]


def export_demand_forecast_vs_actual_parquet(
    out_path: str | None = None,
    region_code: str = "ALL",
    prefer_model_name: str | None = None,
    max_forecast_dates: int | None = None,
) -> dict:
    """
    Export demand forecast vs actual dataset from vw_forecast_vs_actual_all.

    Parameters
    ----------
    out_path : str | None
        Output parquet path. Defaults to:
        data/processed/dashboard/demand_forecast_vs_actual.parquet
    region_code : str
        Region filter (default ALL).
    prefer_model_name : str | None
        Optional filter (e.g. "prophet_v1").
    max_forecast_dates : int | None
        Optional limit on most recent forecast dates.
    """

    # Default output path
    if out_path is None:
        out_dir = PROJECT_ROOT / "data" / "processed" / "dashboard"
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = str(out_dir / "demand_forecast_vs_actual.parquet")

    with get_conn() as conn:
        sql = """
            SELECT
                forecast_date,
                ts_utc,
                metric_code,
                region_code,
                model_name,
                train_days,
                forecast_mw,
                actual_mw,
                error_mw,
                abs_error_mw
            FROM vw_forecast_vs_actual_all
            WHERE metric_code = 'demand_actual'
              AND region_code = ?
        """
        params = [region_code]

        if prefer_model_name:
            sql += " AND model_name = ?"
            params.append(prefer_model_name)

        df = pd.read_sql(sql, conn, params=params)

    if df.empty:
        raise ValueError("vw_forecast_vs_actual_all returned no rows.")

    # Limit forecast dates if requested
    df["forecast_date"] = df["forecast_date"].astype(str)
    if max_forecast_dates is not None:
        keep = sorted(df["forecast_date"].unique())[-int(max_forecast_dates):]
        df = df[df["forecast_date"].isin(keep)].copy()

    # Normalize timestamp
    df["ts_utc"] = pd.to_datetime(df["ts_utc"], utc=True, errors="coerce")

    # Metadata
    df["generated_at_utc"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    # Sort for BI friendliness
    df = df.sort_values(
        ["forecast_date", "ts_utc", "model_name", "train_days"]
    ).reset_index(drop=True)

    df.to_parquet(out_path, index=False)

    summary = {
        "rows": int(len(df)),
        "out_path": out_path,
        "forecast_dates": sorted(df["forecast_date"].unique().tolist()),
        "models": sorted(df["model_name"].unique().tolist()),
        "region_code": region_code,
    }

    print("[dashboard_export] Parquet export complete:")
    print(summary)

    return summary


def main() -> None:
    export_demand_forecast_vs_actual_parquet(
        out_path=None,
        region_code="ALL",
        prefer_model_name=None,     # set to "prophet_v1" if desired
        max_forecast_dates=None,    # e.g. 30 if you want rolling window
    )


if __name__ == "__main__":
    main()
