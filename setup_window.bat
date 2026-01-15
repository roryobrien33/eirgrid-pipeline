@echo off
setlocal EnableExtensions EnableDelayedExpansion

REM ============================================================
REM setup_windows.bat
REM - One-time setup for non-Docker users on Windows
REM - Creates .env from .env.example (if missing)
REM - Initializes SQLite schema, dims, and views
REM ============================================================

set "PROJECT_ROOT=%~dp0"
set "PROJECT_ROOT=%PROJECT_ROOT:~0,-1%"

cd /d "%PROJECT_ROOT%" || exit /b 1

REM Ensure imports work
set "PYTHONPATH=%PROJECT_ROOT%\src"

echo.
echo ============================================================
echo Setup (non-Docker)
echo Project root : %PROJECT_ROOT%
echo PYTHONPATH   : %PYTHONPATH%
echo ============================================================
echo.

if not exist ".env" (
  if exist ".env.example" (
    copy ".env.example" ".env" >nul
    echo Created .env from .env.example.
  ) else (
    echo ERROR: .env.example not found.
    exit /b 1
  )
) else (
  echo .env already exists - leaving as is.
)

echo.
python -m ingest.init_db || exit /b 1
python -m ingest.seed_dims || exit /b 1
python -m ingest.init_views || exit /b 1

echo.
echo SUCCESS: Setup complete.
echo Next: python -m pipeline.daily_forecast_runner --train-days 60
echo.
pause
endlocal
