# GridRelief full demo, A to Z.
# Run from the gridrelief folder:  powershell -ExecutionPolicy Bypass -File .\demo-full.ps1
$json = "application/json"

function Step($n, $title) { Write-Host ""; Write-Host "=== STEP $n : $title ===" -ForegroundColor Cyan }
function Say($text) { Write-Host $text -ForegroundColor Yellow }
function Next { Read-Host "[Press Enter for the next step]" | Out-Null }
function WaitFor($url) {
  for ($i = 0; $i -lt 30; $i++) {
    try { Invoke-RestMethod $url | Out-Null; return } catch { Start-Sleep -Seconds 2 }
  }
}

Step 0 "Fresh start (wipes old data, builds and starts all containers)"
docker compose down
docker compose up --build -d
WaitFor "http://localhost:8001/centers"
WaitFor "http://localhost:8002/requests"
WaitFor "http://localhost:8003/recommendations"
Start-Sleep -Seconds 5
Say "Everything is up. Each service is its own container."
Next

Step 1 "Architecture: 5 containers = independent services"
docker compose ps
Say "dashboard = web console | rabbitmq = message broker | center-service = inventory | request-service = requests | allocation-service = priority engine"
Say "Dashboard: http://localhost:8080   (Swagger: 8001/docs 8002/docs 8003/docs   RabbitMQ UI: 15672)"
Next

Step 2 "Supply: who has shareable stock? (center-service)"
Invoke-RestMethod "http://localhost:8001/inventory?item=food_packets" | Format-Table center_id, name, item, shareable_quantity
Say "Center A offers 500 food packets. Centers B and C have none. Each center owns its own inventory."
Next

Step 3 "FAILURE: stop the allocation service"
docker compose stop allocation-service
docker compose ps
Say "The allocation service is DOWN. The rest of the network keeps running."
Next

Step 4 "Two competing requests while allocation is down"
Say "Center B: 400 food packets, CRITICAL.   Center C: 300 food packets, NORMAL."
Invoke-RestMethod -Method Post -Uri http://localhost:8002/requests -ContentType $json -Body '{"center_id":"center-b","item":"food_packets","quantity":400,"urgency":"critical"}' | Format-Table request_id, center_id, quantity, urgency, status
Invoke-RestMethod -Method Post -Uri http://localhost:8002/requests -ContentType $json -Body '{"center_id":"center-c","item":"food_packets","quantity":300,"urgency":"normal"}' | Format-Table request_id, center_id, quantity, urgency, status
Say "request-service still accepted both. It published request.created events to RabbitMQ."
Next

Step 5 "Proof: events are waiting in the durable queue"
docker compose exec rabbitmq rabbitmqctl list_queues name messages_ready messages_unacknowledged
Say "messages_ready = 2. Nothing is lost. (Also visible at http://localhost:15672 > Queues)"
Next

Step 6 "RECOVERY: bring allocation back"
docker compose start allocation-service
Start-Sleep -Seconds 8
docker compose logs allocation-service --since 1m | Select-String "consuming|handled|recommendation"
Say "On restart it pulled the missed events from the queue and processed them in order."
Next

Step 7 "Result: priority-based allocation"
$recs = Invoke-RestMethod http://localhost:8003/recommendations
$recs | Format-Table request_id, requester, urgency, requested_quantity, recommended_quantity, score, status
foreach ($r in $recs) {
  $parts = foreach ($a in $r.allocations) { "$($a.quantity) from $($a.supplier)" }
  Write-Host ("{0}: {1}" -f $r.request_id, ($parts -join ", "))
}
Say "Demand is 700 but supply is 500. Critical B is served first (400), normal C gets the remaining 100."
Next

Step 8 "Supplier makes the final call (Center A operator)"
Say "Approve B's recommendation in full:"
Invoke-RestMethod -Method Post -Uri http://localhost:8003/recommendations/r-101/decision -ContentType $json -Body '{"decision":"approve","operator":"center-a-operator-1"}' | Select-Object request_id, status, @{n="decided_by";e={$_.decision.operator}}, @{n="time";e={$_.decision.timestamp}} | Format-List
Say "Partially approve C (60 of 100):"
Invoke-RestMethod -Method Post -Uri http://localhost:8003/recommendations/r-102/decision -ContentType $json -Body '{"decision":"partial","quantity":60,"operator":"center-a-operator-1"}' | Select-Object request_id, status, @{n="decided_by";e={$_.decision.operator}}, @{n="time";e={$_.decision.timestamp}} | Format-List
Say "Decision, operator and timestamp are recorded."
Next

Step 9 "Final state"
docker compose ps
Say "Done. Next stages: PostgreSQL per service, transfer + audit services, Kubernetes, Prometheus/Grafana."
