# GridRelief prototype (with live dashboard)

Federated disaster-relief coordination: independent relief centers share limited resources,
competing requests are ranked by one standard rule, the supplying center decides, and the
network keeps working when a service fails.

## Run (Docker Desktop must be running)
    .\start.ps1                       # builds, starts everything, opens the dashboard
    # or: docker compose up --build -d   then open http://localhost:8080
    .\stop.ps1                        # docker compose down

## What runs (5 containers)
| Container | Port | Role |
|---|---|---|
| dashboard | 8080 | Web console: services, event pipeline, centers, requests, live event stream, guided demo |
| center-service | 8001 | Relief centers and inventory (seeded: Center A has 500 food_packets to share) |
| request-service | 8002 | Accepts requests, publishes `request.created` |
| allocation-service | 8003 | Consumes events from a durable queue, ranks by priority, recommends suppliers, records decisions |
| rabbitmq | 5672 / 15672 | Message broker (UI: guest / guest) |

Events on exchange `gridrelief.events`: `inventory.updated`, `request.created`,
`allocation.recommended`, `request.approved`, `request.rejected`.

## Demo in the dashboard (http://localhost:8080)
Use the four numbered buttons at the top, in order:
1. Stop allocation service (its tile turns red; the rest keeps running)
2. Send competing requests (B: 400 critical, C: 300 normal) - accepted even though allocation is down
3. Show retained events (the pipeline shows events waiting in the durable queue, 0 consumers)
4. Restart and recover (queue drains, B gets 400, C gets 100 from Center A)
Then use Approve / Partial / Reject on a recommendation. "Reset demo" starts over.

The Stop / Start / Reset buttons use the Docker Engine API through the mounted
`/var/run/docker.sock`. If you remove that volume line from docker-compose.yml, the dashboard
still works read-only; stop/start the service with `docker compose stop|start allocation-service`.

## Terminal demo (optional)
    powershell -ExecutionPolicy Bypass -File .\demo-full.ps1

## Other URLs
Swagger: http://localhost:8001/docs, 8002/docs, 8003/docs. RabbitMQ UI: http://localhost:15672.

## Honest limits (TODO)
- All data is in memory; restarting a service clears its data. No PostgreSQL yet.
- Approvals are recorded and published as events, but stock is not yet deducted in center-service;
  the dashboard's "reserved" figure is derived from approved recommendations.
- Not built: authentication, verified registration, transfer-service (dispatch/receipt),
  audit-service, two-person approval, Prometheus/Grafana, Kubernetes.
- The dashboard container mounts the Docker socket: fine for a local demo, not for production.
- Priority formula is a configurable starting rule, not a validated humanitarian standard.
