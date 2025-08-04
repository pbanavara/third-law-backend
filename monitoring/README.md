# Monitoring Setup

This directory contains the monitoring setup for the FastAPI application using Prometheus and Grafana.

## Components

- **Prometheus**: Metrics collection and storage
- **Grafana**: Metrics visualization and dashboards

## Setup

1. Install Docker and Docker Compose if not already installed.

2. Start the monitoring stack:
```bash
cd monitoring
docker-compose up -d
```

3. Access the services:
   - Prometheus: http://localhost:9090
   - Grafana: http://localhost:4000 (admin/admin)

## Metrics

The FastAPI application exposes metrics at `/metrics` endpoint. Key metrics include:

- HTTP request counts
- Request duration
- Response status codes
- System metrics (CPU, Memory)

## Grafana Dashboards

The default dashboard includes:
- Request Rate by endpoint
- Average Response Time
- Error Rate
- Status Code distribution

## Adding Custom Metrics

To add custom metrics, modify the FastAPI application using the `prometheus_fastapi_instrumentator` library:

```python
from prometheus_fastapi_instrumentator import Instrumentator

# Add custom metrics
Instrumentator().instrument(app).expose(app, include_in_schema=True)
```

## Troubleshooting

1. If metrics aren't showing up:
   - Check if the FastAPI app is accessible from Prometheus (host.docker.internal)
   - Verify the /metrics endpoint is working
   - Check Prometheus targets page for scrape status

2. If Grafana can't connect to Prometheus:
   - Check if Prometheus is running
   - Verify the datasource configuration
   - Check network connectivity between containers 