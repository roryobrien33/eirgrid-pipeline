# Import statements
from datetime import timezone
from zoneinfo import ZoneInfo
import pandas as pd
import sqlite3
from pathlib import Path

def initialize_db():
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

    # Connect to the DB and Turn FK's on
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("PRAGMA foreign_keys = ON;")

    # Read Schema and execute the Query script
        schema_cmd = SCHEMA_PATH.read_text(encoding="utf-8")
        conn.executescript(schema_cmd)
    # TEST
        test_query = conn.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;")
        print(test_query.fetchall())

initialize_db()