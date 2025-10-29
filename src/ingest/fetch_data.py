import requests          # for API calls (HTTP GET requests)
import time              # for retry delays (sleep / backoff)
from datetime import datetime, date, timedelta   # for looping days and date formatting
import pandas as pd      # for building + transforming DataFrames
from zoneinfo import ZoneInfo   # for time zone handling (Europe/Dublin → UTC)
from pathlib import Path        # for file paths (if you save JSON or need DB paths)
from ingest.stage import stage_readings   # to stage the tidy data into SQLite

BASE_URL = "https://www.smartgriddashboard.com/api/chart/"
DEFAULT_REGION = "ALL"
DEFAULT_CHART_TYPE = "default"
DEFAULT_DATERANGE = "day"
REQUEST_TIMEOUT_S = 15
MAX_RETRIES = 3
BACKOFF_S = [1, 2, 4]
DEFAULT_HEADERS =

