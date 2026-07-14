from dataclasses import dataclass, field
from typing import Optional, Any


@dataclass
class DeploymentEvent:
    location_name: str
    image_tag: str
    snapshot_json: Optional[str] = None
    commit_hash: Optional[str] = None
    k8s_config: dict[str, Any] = field(default_factory=dict)
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
