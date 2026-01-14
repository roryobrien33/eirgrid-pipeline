"""
Apply SQLite views to the project database.

This is intentionally separate from init_db.py so database schema creation and
view creation can be run independently and re-run safely.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path


def apply_views(db_path: Path, views_sql_path: Path) -> None:
    if not db_path.exists():
        raise FileNotFoundError(f"Database not found: {db_path}")

    if not views_sql_path.exists():
        raise FileNotFoundError(f"views.sql not found: {views_sql_path}")

    sql = views_sql_path.read_text(encoding="utf-8")

    con = sqlite3.connect(str(db_path))
    try:
        con.executescript(sql)
        con.commit()
    finally:
        con.close()


def main() -> None:
    project_root = Path(__file__).resolve().parents[2]  # .../src/ingest -> repo root
    db_path = project_root / "db" / "eirgrid.db"
    views_sql_path = project_root / "db" / "views.sql"

    print(f"DB Path: {db_path}")
    print(f"Views SQL: {views_sql_path}")

    apply_views(db_path, views_sql_path)
    print("✅ Views applied (safe to re-run).")


if __name__ == "__main__":
    main()
