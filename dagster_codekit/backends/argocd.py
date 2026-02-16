"""
ArgoCD backend for dagster-codekit.

Processes ArgoCD webhook notifications and extracts Dagster location info
from Application labels.
"""

import hmac
from typing import Optional, Any

import structlog
from starlette.requests import Request

from dagster_codekit.exceptions import AuthenticationError, ConfigurationError, ValidationError
from dagster_codekit.interfaces import BackendPlugin
from dagster_codekit.models import DeploymentEvent
from dagster_codekit.utils.health import wait_for_grpc_server

logger = structlog.get_logger()


class ArgoCDBackend(BackendPlugin):
    """
    ArgoCD webhook backend.
    Extracts Dagster location information from ArgoCD Application labels.
    """

    def name(self) -> str:
        return "argocd"

    def validate_signature(self, request: Request) -> bool:
        """
        Validate ArgoCD webhook signature (X-Argocd-Webhook-Secret).
        """
        webhook_secret = self.config.get("webhook_secret")
        if not webhook_secret:
            raise ConfigurationError("ArgoCD webhook_secret not configured.")

        # ArgoCD sends secret in header (simple comparison, not HMAC hash)
        received_secret = request.headers.get("X-Argocd-Webhook-Secret", "")

        # Constant-time comparison to prevent timing attacks
        if not hmac.compare_digest(webhook_secret, received_secret):
            raise AuthenticationError("Invalid ArgoCD webhook secret.")

        return True

    async def parse_event(self, payload: dict[str, Any]) -> Optional[DeploymentEvent]:
        """
        Parse ArgoCD webhook payload.
        Returns DeploymentEvent if labels match, else None.
        """
        app = payload.get("app", {})
        if not app:
            raise ValidationError("Invalid payload: missing 'app' field.")

        metadata = app.get("metadata", {})
        labels = metadata.get("labels", {})
        name = metadata.get("name", "unknown")

        # 1. Filter: Is this a Dagster location?
        if labels.get("dagster.io/code-location") != "true":
            logger.debug("argocd_ignored_no_label", app=name)
            return None

        # 2. Filter: Is the app synced and healthy?
        status = app.get("status", {})
        sync_status = status.get("sync", {}).get("status")
        health_status = status.get("health", {}).get("status")

        if sync_status != "Synced" or health_status != "Healthy":
            logger.debug("argocd_ignored_status", app=name, sync=sync_status, health=health_status)
            return None

        # 3. Validation: Required labels
        location_name = labels.get("dagster.io/location-name")
        if not location_name:
            raise ConfigurationError(
                f"App '{name}' missing required label 'dagster.io/location-name'"
            )

        # 4. Extraction: Host/Port
        # Default convention: <app-name>.<namespace>.svc.cluster.local
        namespace = metadata.get("namespace", "dagster")
        default_host = f"{name}.{namespace}.svc.cluster.local"

        grpc_host = labels.get("dagster.io/grpc-host", default_host)
        grpc_port = int(labels.get("dagster.io/grpc-port", "4000"))

        return DeploymentEvent(
            location_name=location_name,
            grpc_host=grpc_host,
            grpc_port=grpc_port,
            namespace=namespace,
            metadata={"argocd_app": name, "source": "argocd"},
        )

    async def wait_ready(self, event: DeploymentEvent) -> bool:
        """
        Delegates to generic gRPC health check utility.
        """
        timeout = self.config.get("grpc_timeout", 60)

        return await wait_for_grpc_server(
            host=event.grpc_host,
            port=event.grpc_port,
            timeout=timeout,
            location_name=event.location_name,
            logger=logger,
        )
