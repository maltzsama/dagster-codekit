from dataclasses import dataclass, field
from typing import Optional, Any


@dataclass
class DeploymentEvent:
    """
    Standardized deployment event from any CI/CD backend.

    This is THE contract between backends and the core engine.
    All backends must produce this, regardless of their specific payload format.
    """

    location_name: str
    image_tag: str
    snapshot_json: Optional[str] = None
    commit_hash: Optional[str] = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        if not self.location_name:
            raise ValueError("location_name is required")


@dataclass
class ValidationResult:
    """
    Result from a validator plugin.
    Used by the ValidatorPlugin interface to signal success or failure.
    """

    success: bool
    message: str
