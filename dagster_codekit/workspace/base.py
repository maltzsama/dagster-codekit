from abc import ABC, abstractmethod
from typing import Optional, Any
from dagster_codekit.exceptions import ValidationError


class WorkspaceManager(ABC):
    """Abstract base class for workspace management."""

    @abstractmethod
    def add_or_update(self, location_name: str, location_config: dict[str, Any]) -> None:
        pass

    @abstractmethod
    async def validate(self) -> None:
        """Check if workspace storage is accessible and writable."""
        pass

    def _find_location_index(self, workspace: dict[str, Any], location_name: str) -> Optional[int]:
        for i, loc in enumerate(workspace.get("load_from", [])):
            loc_name = (
                loc.get("grpc_server", {}).get("location_name")
                or loc.get("python_package", {}).get("location_name")
                or loc.get("python_file", {}).get("location_name")
                or loc.get("docker_image", {}).get("location_name")
            )
            if loc_name == location_name:
                return i
        return None

    def _validate_location_config(self, location_config: dict[str, Any]) -> None:
        if not isinstance(location_config, dict):
            raise ValidationError(f"location_config must be a dict, got {type(location_config)}")

        valid_keys = {"grpc_server", "python_package", "python_file", "docker_image"}
        if not any(key in location_config for key in valid_keys):
            raise ValidationError(f"Invalid location_config keys: {list(location_config.keys())}")
