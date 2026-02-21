# Example observability stack (OTel Collector, Jaeger, Prometheus)

This directory runs an OpenTelemetry Collector, intended as an example or for testing new telemetry data. The collector receives telemetry from Tangle and forwards traces to Jaeger and metrics to Prometheus.

## Start the stack

From this directory:

```bash
docker-compose up -d
```

To view logs:

```bash
docker-compose logs -f
```

To stop:

```bash
docker-compose down
```

## Configure Tangle to export to the collector

Set these environment variables when running the Tangle API so it sends traces and metrics to the collector:

| Variable | Value | Description |
|----------|--------|-------------|
| `TANGLE_OTEL_EXPORTER_ENDPOINT` | `http://localhost:4317` | OTLP collector address (gRPC). Use `host.docker.internal:4317` if Tangle runs inside Docker. |
| `TANGLE_OTEL_EXPORTER_PROTOCOL` | `grpc` | Protocol for the exporter (default). This stack only accepts gRPC. |
| `TANGLE_ENV` | optional | Included in the service name (e.g. `tangle-development`). Default: `development`. |

Example (shell):

```bash
export TANGLE_OTEL_EXPORTER_ENDPOINT=http://localhost:4317
export TANGLE_OTEL_EXPORTER_PROTOCOL=grpc
# then start your Tangle API
```

Example (`.env` in the project root or where the API is started):

```
TANGLE_OTEL_EXPORTER_ENDPOINT=http://localhost:4317
TANGLE_OTEL_EXPORTER_PROTOCOL=grpc
```

If `TANGLE_OTEL_EXPORTER_ENDPOINT` is unset, tracing and metrics export are disabled.

## UIs

- **Jaeger** (traces): http://localhost:16686  
- **Prometheus** (metrics): http://localhost:9090  

## Ports

| Port | Service | Purpose |
|------|---------|---------|
| 4317 | OTel Collector | OTLP gRPC (ingest) |
| 9464 | OTel Collector | Prometheus scrape endpoint |
| 8888 | OTel Collector | Collector telemetry |
| 16686 | Jaeger | Web UI |
| 9090 | Prometheus | Web UI |
