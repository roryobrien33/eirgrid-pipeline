Param(
  [int]$TrainDays = 60,
  [switch]$NoBuild,
  [switch]$Clean
)

$ErrorActionPreference = "Stop"

try {
  if ($Clean) {
    docker compose down --remove-orphans | Out-Host

    Remove-Item -Recurse -Force .\data\processed\forecasts\* -ErrorAction SilentlyContinue
    Remove-Item -Recurse -Force .\data\processed\dashboard\* -ErrorAction SilentlyContinue
    Remove-Item -Force .\db\eirgrid.db -ErrorAction SilentlyContinue
  }

  if (-not $NoBuild) {
    docker compose build | Out-Host
  }

  docker compose run --rm `
    -e "TRAIN_DAYS=$TrainDays" `
    eirgrid

  exit $LASTEXITCODE
}
catch {
  Write-Error $_
  exit 1
}
