"""Prometheus metrics for dagster-codekit."""

from prometheus_client import Counter, Gauge, generate_latest, REGISTRY

deployments_total = Counter(
    "codekit_deployments_total",
    "Total number of deployment events received",
    ["location"],
)

runs_launched_total = Counter(
    "codekit_runs_launched_total",
    "Total number of runs launched",
    ["location", "launcher"],
)

grpc_requests_total = Counter(
    "codekit_grpc_requests_total",
    "Total number of gRPC requests served",
    ["method"],
)

locations_count = Gauge(
    "codekit_locations_count",
    "Number of registered code locations",
)

snapshots_count = Gauge(
    "codekit_snapshots_count",
    "Total number of snapshots stored",
)


def get_metrics_response() -> bytes:
    return generate_latest(REGISTRY)
