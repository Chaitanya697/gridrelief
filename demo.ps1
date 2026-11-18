# Creates the two competing requests (run after: docker compose stop allocation-service)
$json = "application/json"
Invoke-RestMethod -Method Post -Uri http://localhost:8002/requests -ContentType $json `
  -Body '{"center_id":"center-b","item":"food_packets","quantity":400,"urgency":"critical"}'
Invoke-RestMethod -Method Post -Uri http://localhost:8002/requests -ContentType $json `
  -Body '{"center_id":"center-c","item":"food_packets","quantity":300,"urgency":"normal"}'
