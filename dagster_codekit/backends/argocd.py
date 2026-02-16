import hmac
import structlog
from typing import Any, Optional

from starlette.requests import Request

from dagster_codekit.interfaces import BackendPlugin
from dagster_codekit.models import DeploymentEvent
from dagster_codekit.exceptions import AuthenticationError, ValidationError, ConfigurationError
from dagster_codekit.utils.health import wait_for_grpc_server
from dagster_codekit.config import ArgoCDBackendConfig

logger = structlog.get_logger()


class ArgoCDBackend(BackendPlugin):

    def __init__(self, config: ArgoCDBackendConfig):

        self.config = config

    def name(self) -> str:
        return "argocd"

    def validate_signature(self, request: Request) -> bool:

        webhook_secret = self.config.webhook_secret

        received_secret = request.headers.get("X-Argocd-Webhook-Secret", "")

        if not hmac.compare_digest(webhook_secret, received_secret):
            raise AuthenticationError("Invalid ArgoCD webhook secret.")

        return True

    async def parse_event(self, payload: dict[str, Any]) -> Optional[DeploymentEvent]:

        app = payload.get("app", {})
        if not app:
            raise ValidationError("Invalid payload: missing 'app' field.")

        metadata = app.get("metadata", {})
        labels = metadata.get("labels", {})
        name = metadata.get("name", "unknown")

        if labels.get("dagster.io/code-location") != "true":
            logger.debug("argocd_ignored_no_label", app=name)
            return None

        status = app.get("status", {})
        sync_status = status.get("sync", {}).get("status")
        health_status = status.get("health", {}).get("status")

        if sync_status != "Synced" or health_status != "Healthy":
            logger.debug("argocd_ignored_status", app=name, sync=sync_status, health=health_status)
            return None

        location_name = labels.get("dagster.io/location-name")
        if not location_name:
            raise ConfigurationError(
                f"App '{name}' missing required label 'dagster.io/location-name'"
            )

        namespace = metadata.get("namespace", "dagster")
        default_host = f"{name}.{namespace}.svc.cluster.local"

        grpc_host = labels.get("dagster.io/grpc-host", default_host)
        grpc_port = int(labels.get("dagster.io/grpc-port", "4000"))

        return DeploymentEvent(
            location_name=location_name,
            grpc_host=grpc_host,
            grpc_port=grpc_port,
            namespace=namespace,
            deployment_type="grpc-service",
            metadata={"argocd_app": name, "source": "argocd"},
        )

    async def wait_ready(self, event: DeploymentEvent) -> bool:

        return await wait_for_grpc_server(
            host=event.grpc_host,
            port=event.grpc_port,
            timeout=self.config.grpc_timeout,
            use_tls=self.config.grpc_tls,
            check_interval=self.config.check_interval,
        )
