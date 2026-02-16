"""
Core data models for dagster-codekit.
These are the contracts between different components.
"""

from dataclasses import dataclass, field
from typing import Any, Literal, Optional


@dataclass
class DeploymentEvent:
    """
    Standardized deployment event from any CI/CD backend.

    This is THE contract between backends and the core engine.
    All backends must produce this, regardless of their specific payload format.
    """

    # REQUIRED fields
    location_name: str  # Dagster code location name
    grpc_host: str  # gRPC server hostname
    grpc_port: int  # gRPC server port

    # OPTIONAL fields
    commit_hash: Optional[str] = None
    image: Optional[str] = None
    namespace: Optional[str] = None
    deployment_type: Literal["grpc-service", "docker-image"] = "grpc-service"
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        """Fail fast validation."""
        if not self.location_name:
            raise ValueError("DeploymentEvent: location_name cannot be empty")

        if not self.grpc_host:
            raise ValueError("DeploymentEvent: grpc_host cannot be empty")

        if self.grpc_port <= 0 or self.grpc_port > 65535:
            raise ValueError(f"DeploymentEvent: Invalid grpc_port: {self.grpc_port}")


@dataclass
class ValidationResult:
    """
    Result from a validator plugin.
    Used by the ValidatorPlugin interface to signal success or failure.
    """

    success: bool
    message: str


@dataclass
class LocationInfo:
    """
    Internal representation of a Code Location configuration.

    Used primarily by the GitHub backend to map repositories to
    intended locations before a deployment event occurs.
    """

    name: str
    grpc_host: str
    grpc_port: int
    path: Optional[str] = None
