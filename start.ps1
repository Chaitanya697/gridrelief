# Starts the whole GridRelief network and opens the dashboard.
docker compose up --build -d
Write-Host "Waiting for the dashboard..."
for ($i = 0; $i -lt 30; $i++) {
  try { Invoke-RestMethod http://localhost:8080/api/state | Out-Null; break } catch { Start-Sleep -Seconds 2 }
}
Start-Process "http://localhost:8080"
Write-Host "Dashboard: http://localhost:8080"
