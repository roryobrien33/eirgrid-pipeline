@echo off
setlocal

REM Run from repo root
cd /d "%~dp0"

REM Ensure Docker is available
docker version >nul 2>&1
if errorlevel 1 (
  echo Docker is not running or not installed.
  echo Start Docker Desktop, then run this again.
  exit /b 1
)

REM Optional: create .env if missing (copy from example)
if not exist ".env" (
  if exist ".env.example" (
    copy ".env.example" ".env" >nul
    echo Created .env from .env.example
  )
)

echo Building image (first time may take a few minutes)...
docker compose build
if errorlevel 1 exit /b 1

echo Running pipeline container (one-shot)...
docker compose run --rm eirgrid
if errorlevel 1 exit /b 1

echo Done. Outputs should be in:
echo   data\processed\forecasts
echo   data\processed\dashboard
exit /b 0
