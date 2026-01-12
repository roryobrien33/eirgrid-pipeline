@echo off
setlocal EnableExtensions

REM ============================================================
REM run_daily_forecast.bat
REM - Runs the daily ETL + next-day forecasts
REM - Forces a specific conda env python (avoids Windows alias issues)
REM - Sets PYTHONPATH so python -m pipeline... resolves correctly
REM ============================================================

REM --- Update these if you move the project/env ---
set "PROJECT_ROOT=C:\Users\roryo\Documents\DATA-Portfolio\eirgrid-pipeline"
set "PYTHON_EXE=C:\Users\roryo\anaconda3\envs\eirgrid-project\python.exe"

REM --- Runtime options ---
set "TRAIN_DAYS=60"

REM --- Ensure imports work regardless of working directory ---
set "PYTHONPATH=%PROJECT_ROOT%\src"

REM --- Optional: keep temp files inside the repo (recommended for reproducibility) ---
set "TMP=%PROJECT_ROOT%\tmp"
set "TEMP=%PROJECT_ROOT%\tmp"
if not exist "%PROJECT_ROOT%\tmp" mkdir "%PROJECT_ROOT%\tmp"

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
if not exist "%PYTHON_EXE%" (
  echo ERROR: PYTHON_EXE not found: "%PYTHON_EXE%"
  pause
  exit /b 1
)

if not exist "%PROJECT_ROOT%\src" (
  echo ERROR: src directory not found: "%PROJECT_ROOT%\src"
  pause
  exit /b 1
)

REM --- Run from src (nice-to-have); PYTHONPATH already handles imports ---
cd /d "%PROJECT_ROOT%\src"
if errorlevel 1 (
  echo ERROR: Failed to cd into "%PROJECT_ROOT%\src"
  pause
  exit /b 1
)

REM --- Print the interpreter actually being used ---
echo Verifying interpreter...
"%PYTHON_EXE%" -c "import sys; print('sys.executable=', sys.executable); print('sys.version=', sys.version.split()[0])"
if errorlevel 1 (
  echo ERROR: Failed interpreter sanity check.
  pause
  exit /b 1
)

echo.
echo Running: python -m pipeline.daily_forecast_runner --train-days %TRAIN_DAYS%
echo.

"%PYTHON_EXE%" -m pipeline.daily_forecast_runner --train-days %TRAIN_DAYS%
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
