"""
File-based workspace manager.

Manages workspace.yaml file with atomic writes and file locking.
"""

import fcntl
import os
from pathlib import Path

import structlog
from ruamel.yaml import YAML

from dagster_codekit.core.exceptions import ConfigurationError

logger = structlog.get_logger()


class FileWorkspaceManager:
    """
    Manage workspace via file-based workspace.yaml.

    Features:
    - Thread-safe file locking (fcntl)
    - Atomic writes (temp file + os.replace)
    - Preserves YAML formatting and comments (ruamel.yaml)

    Usage:
        manager = FileWorkspaceManager("/opt/dagster/workspace.yaml")
        manager.add_or_update("analytics", {
            "grpc_server": {
                "host": "analytics.svc",
                "port": 4000,
                "location_name": "analytics"
            }
        })
    """

    def __init__(self, path: str):
        """
        Initialize file-based workspace manager.

        Args:
            path: Path to workspace.yaml file

        Raises:
            ConfigurationError: If file doesn't exist or is invalid
        """
        self.path = Path(path)
        self.yaml = YAML()
        self.yaml.preserve_quotes = True
        self.yaml.default_flow_style = False

        # Ensure file exists
        if not self.path.exists():
            raise ConfigurationError(
                f"Workspace file not found: {self.path}\n\n"
                f"Create it with:\n"
                f"  echo 'load_from: []' > {self.path}\n"
            )

        logger.info(
            "workspace_manager_initialized",
            mode="file",
            path=str(self.path),
        )

    def add_or_update(self, location_name: str, location_config: dict) -> None:
        """
        Add or update location in workspace.yaml.

        Thread-safe with file locking. Uses atomic writes.

        Args:
            location_name: Name of the code location
            location_config: Location configuration dict

        Raises:
            ConfigurationError: If workspace file is invalid
        """
        # Validate location_config structure
        self._validate_location_config(location_config)

        # Open file with exclusive lock
        with open(self.path, "r+") as f:
            try:
                # Acquire exclusive lock
                fcntl.flock(f.fileno(), fcntl.LOCK_EX)

                # Read current workspace
                workspace = self.yaml.load(f)

                if workspace is None or "load_from" not in workspace:
                    workspace = {"load_from": []}

                # Find existing location
                existing_idx = self._find_location_index(workspace, location_name)

                if existing_idx is not None:
                    logger.info(
                        "file_location_updating",
                        name=location_name,
                        index=existing_idx,
                        total_locations=len(workspace["load_from"]),
                    )
                    workspace["load_from"][existing_idx] = location_config
                else:
                    logger.info(
                        "file_location_adding",
                        name=location_name,
                        new_total=len(workspace["load_from"]) + 1,
                    )
                    workspace["load_from"].append(location_config)

                # Write to temp file first (atomic write)
                temp_path = self.path.with_suffix(".tmp")

                with open(temp_path, "w") as temp_f:
                    self.yaml.dump(workspace, temp_f)

                # Atomic replace
                os.replace(temp_path, self.path)

                logger.info(
                    "file_workspace_updated",
                    location=location_name,
                    total_locations=len(workspace["load_from"]),
                    path=str(self.path),
                )

            finally:
                # Release lock
                fcntl.flock(f.fileno(), fcntl.LOCK_UN)

    # In FileWorkspaceManager
    async def validate(self) -> None:
        if not os.path.exists(self.path):
            try:
                # Try to create a default one if it doesn't exist
                with open(self.path, "w") as f:
                    f.write("load_from: []\n")
                logger.info("workspace_file_created", path=self.path)
            except Exception as e:
                raise ConfigurationError(
                    f"Workspace file not found and could not be created at {self.path}: {e}"
                )

        if not os.access(self.path, os.W_OK):
            raise ConfigurationError(
                f"Workspace file at {self.path} is not writable. Check permissions."
            )

        logger.info("workspace_validation_success", path=self.path)

    def _find_location_index(self, workspace: dict, location_name: str) -> int | None:
        """
        Find location index by name.

        Args:
            workspace: Workspace config dict
            location_name: Location name to find

        Returns:
            Index if found, None otherwise
        """
        for i, loc in enumerate(workspace.get("load_from", [])):
            loc_name = (
                loc.get("grpc_server", {}).get("location_name")
                or loc.get("python_package", {}).get("location_name")
                or loc.get("python_file", {}).get("location_name")
            )

            if loc_name == location_name:
                return i

        return None

    def _validate_location_config(self, location_config: dict) -> None:
        """
        Validate location config structure.

        Args:
            location_config: Config to validate

        Raises:
            ConfigurationError: If config is invalid
        """
        if not isinstance(location_config, dict):
            raise ConfigurationError(f"location_config must be a dict, got {type(location_config)}")

        # Must have at least one of these
        if (
            "grpc_server" not in location_config
            and "python_package" not in location_config
            and "python_file" not in location_config
        ):
            raise ConfigurationError(
                f"Invalid location_config: must contain either 'grpc_server', "
                f"'python_package', or 'python_file'.\nGot: {location_config}"
            )
