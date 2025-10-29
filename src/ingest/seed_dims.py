import sqlite3
from pathlib import Path

def seed_dimensions():
    # Investigate the path of this file
    INIT_DB_PATH = Path(__file__)
    # Test
    print(f"INIT_DB absolute path: {INIT_DB_PATH.resolve()}")
    print("")

    # Move up the file path to the correct folder
    PROJECT_ROOT = INIT_DB_PATH.resolve().parents[2]

    # Build the file path for the schema and database files
    SCHEMA_PATH = PROJECT_ROOT / "db" / "schema.sql"
    DB_PATH = PROJECT_ROOT / "db" / "eirgrid.db"
    # Test
    print(f"DB Path type: {type(DB_PATH)}, SCHEMA Path type: {type(SCHEMA_PATH)}")
    print("")
    print(f"DB Path absolute path: {DB_PATH.resolve()}, SCHEMA Path absolute path: {SCHEMA_PATH.resolve()}")
    print("")

    # Create file directory (if not exist)
    SCHEMA_PATH.parent.mkdir(parents=True, exist_ok=True)
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    # Test
    print(f"SCHEMA PARENT EXISTS: {SCHEMA_PATH.parent.exists()}")
    print(f"DB PARENT EXISTS: {DB_PATH.parent.exists()}")

    # Create lists for the data metrics and regions
    metrics_list = [("wind_actual", "MW"), ("solar_actual", "MW"), ("demand_actual", "MW")]
    regions_list = [("ALL",)]
    # Test
    print(f"Metrics list: {metrics_list}, Regions list: {regions_list}")

    # Connect to the DB and Turn FK's on
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("PRAGMA foreign_keys = ON;")

        # Insert metric and region lists into the DB
        conn.executemany("INSERT OR IGNORE INTO dim_metric (metric_code,unit) VALUES (?,?);", metrics_list,)
        conn.executemany("INSERT OR IGNORE INTO dim_region (region_code) VALUES (?);", regions_list,)

        # Test
        metrics = conn.execute(
            "SELECT metric_code, unit FROM dim_metric ORDER BY metric_code;"
        ).fetchall()

        regions = conn.execute(
            "SELECT region_code FROM dim_region ORDER BY region_code;"
        ).fetchall()

        print("Metrics table contents:")
        for row in metrics:
            print(" ", row)

        print("\nRegions table contents:")
        for row in regions:
            print(" ", row)

    print("\n✅ Seeding complete (safe to re-run).")

# Allow script to run standalone
if __name__ == "__main__":
    seed_dimensions()




