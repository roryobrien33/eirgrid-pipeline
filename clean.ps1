docker compose down --remove-orphans
Remove-Item -Recurse -Force .\data\processed\forecasts\* -ErrorAction SilentlyContinue
Remove-Item -Recurse -Force .\data\processed\dashboard\* -ErrorAction SilentlyContinue
Remove-Item -Force .\db\eirgrid.db -ErrorAction SilentlyContinue
Write-Host "Clean complete."
