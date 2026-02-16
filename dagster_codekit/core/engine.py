"""
Deployment engine for dagster-codekit.

Orchestrates the deployment flow: update -> reload.
"""

from typing import Any, Dict

import structlog

from dagster_codekit import DeploymentEvent
from dagster_codekit.utils.reloader import DagsterReloader
from dagster_codekit.workspace.base import WorkspaceManager
from structlog.contextvars import bind_contextvars

logger = structlog.get_logger()


class DeploymentEngine:
    """
    Orchestrates the deployment lifecycle.
    """

    def __init__(
        self,
        workspace_manager: WorkspaceManager,
        reloader: DagsterReloader,
    ):
        self.workspace_manager = workspace_manager
        self.reloader = reloader

    async def process_deployment(self, event: DeploymentEvent) -> None:
        """
        Execute the deployment pipeline.
        Failures here will bubble up to the server causing HTTP 500.
        """
        # 1. Create Contextual Logger
        bind_contextvars(
            location=event.location_name,
            grpc_host=event.grpc_host,
            commit=event.commit_hash or "unknown",
            deploy_type=event.deployment_type,
        )

        logger.info("deployment_started")

        # STEP 1: Build Config Object
        location_config = self._build_location_config(event)

        # STEP 2: Persist to Workspace (Atomic)
        logger.debug("updating_workspace_config")
        self.workspace_manager.add_or_update(event.location_name, location_config)
        logger.info("workspace_updated")

        # STEP 3: Trigger Dagster Reload
        logger.debug("triggering_dagster_reload")
        await self.reloader.reload()

        logger.info("dagster_reloaded")
        logger.info("deployment_completed_successfully")

    def _build_location_config(self, event: DeploymentEvent) -> Dict[str, Any]:
        """
        Constructs the strict dictionary expected by Dagster's workspace.yaml.
        """
        if event.deployment_type == "grpc-service":
            return {
                "grpc_server": {
                    "host": event.grpc_host,
                    "port": event.grpc_port,
                    "location_name": event.location_name,
                }
            }

        elif event.deployment_type == "docker-image":
            if not event.image:
                raise ValueError("Deployment type is 'docker-image' but 'image' field is missing")
            return {
                "docker_image": {
                    "image": event.image,
                    "location_name": event.location_name,
                }
            }

        raise ValueError(f"Unsupported deployment_type: {event.deployment_type}")
