from __future__ import annotations

from ingest.promote import get_conn


TABLES = ["dim_metric", "dim_region", "fact_readings", "fact_forecasts"]


def print_table_info(cur, table: str) -> None:
    print("\n" + "=" * 60)
    print(table)

    print("-- table_info (cid, name, type, notnull, dflt_value, pk)")
    for row in cur.execute(f"PRAGMA table_info({table});"):
        print(row)

    print("-- indexes (seq, name, unique, origin, partial)")
    for row in cur.execute(f"PRAGMA index_list({table});"):
        print(row)

    print("-- foreign_keys (id, seq, table, from, to, on_update, on_delete, match)")
    for row in cur.execute(f"PRAGMA foreign_key_list({table});"):
        print(row)

    print("-- create_sql")
    row = cur.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name=?;", (table,)
    ).fetchone()
    print(row[0] if row and row[0] else None)


def main() -> None:
    conn = get_conn()
    try:
        cur = conn.cursor()
        for t in TABLES:
            print_table_info(cur, t)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
