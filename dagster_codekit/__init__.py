"""
dagster-codekit - Serverless Metadata Control Plane for Dagster OSS.

Decouples Code Definitions (CI/CD) from Execution (Kubernetes), enabling
"Zero-Infrastructure" deployments via a gRPC Proxy architecture.

Components:
- CLI: Extracts metadata snapshots during CI/CD.
- Proxy: Serves metadata to Dagster Webserver without loading user code.
- Launcher: Spawns ephemeral Kubernetes jobs for execution.
"""

from dagster_codekit.__version__ import __version__

# Os schemas usados na comunicação CI <-> Proxy
from dagster_codekit.api.schemas import DeploymentEvent

# Exceptions que podem estourar no cliente (CLI)
from dagster_codekit.core.exceptions import (
    AuthenticationError,
    CodekitError,
    ConfigurationError,
    ConnectionError,
)

__all__ = [
    "__version__",
    "CodekitError",
    "ConfigurationError",
    "AuthenticationError",
    "ConnectionError",
    "DeploymentEvent",
]
