@echo off
setlocal EnableExtensions EnableDelayedExpansion

REM ============================================================
REM run_daily_forecast.bat
REM - Runs the daily ETL + next-day forecasts
REM - Uses the active Python interpreter (conda / venv)
REM - Resolves paths dynamically (no hard-coded user paths)
REM ============================================================

REM --- Resolve project root from this script's location ---
REM This assumes the .bat file lives in the project root
set "PROJECT_ROOT=%~dp0"
REM Remove trailing backslash if present
if "%PROJECT_ROOT:~-1%"=="\" set "PROJECT_ROOT=%PROJECT_ROOT:~0,-1%"

REM --- Python executable (from active environment) ---
set "PYTHON_EXE=python"

REM --- Runtime options ---
set "TRAIN_DAYS=60"

REM --- Ensure imports work regardless of working directory ---
set "PYTHONPATH=%PROJECT_ROOT%\src"

REM --- Keep temp files inside the repo ---
set "TMP=%PROJECT_ROOT%\tmp"
set "TEMP=%PROJECT_ROOT%\tmp"
if not exist "%TMP%" mkdir "%TMP%"

echo.
echo ============================================================
echo Running daily forecast runner
echo Project root : %PROJECT_ROOT%
echo Working dir  : %PROJECT_ROOT%\src
echo PYTHON_EXE   : %PYTHON_EXE%
echo PYTHONPATH   : %PYTHONPATH%
echo TMP/TEMP     : %TMP%
echo ============================================================
echo.

REM --- Safety checks ---
where %PYTHON_EXE% >nul 2>&1
if errorlevel 1 (
  echo ERROR: Python executable not found in PATH.
  echo Activate the correct environment before running this script.
  pause
  exit /b 1
)

if not exist "%PROJECT_ROOT%\src" (
  echo ERROR: src directory not found: "%PROJECT_ROOT%\src"
  pause
  exit /b 1
)

REM --- Run from src (PYTHONPATH already set) ---
cd /d "%PROJECT_ROOT%\src"
if errorlevel 1 (
  echo ERROR: Failed to cd into "%PROJECT_ROOT%\src"
  pause
  exit /b 1
)

REM --- Print interpreter details ---
echo Verifying interpreter...
%PYTHON_EXE% -c "import sys; print('sys.executable=', sys.executable); print('sys.version=', sys.version.split()[0])"
if errorlevel 1 (
  echo ERROR: Failed interpreter sanity check.
  pause
  exit /b 1
)

echo.
echo Running: python -m pipeline.daily_forecast_runner --train-days %TRAIN_DAYS%
echo.

%PYTHON_EXE% -m pipeline.daily_forecast_runner --train-days %TRAIN_DAYS%
set "EXITCODE=%ERRORLEVEL%"

echo.
if not "%EXITCODE%"=="0" (
  echo ERROR: daily_forecast_runner failed with exit code %EXITCODE%
  pause
  exit /b %EXITCODE%
)

echo SUCCESS: daily_forecast_runner completed.
pause
endlocal
