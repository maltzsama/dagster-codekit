"""Forgejo (Gitea fork) CI/CD webhook backend.

Supports two payload modes:
- custom: a simple JSON payload sent from a Forgejo Actions workflow step
  after build/push succeeds. Authenticated via HMAC-SHA256 over body.
- native: Forgejo's native webhook payload (workflow_job or package events).

Authentication via X-Forgejo-Webhook-Secret header using HMAC-SHA256.
"""

import hmac
from typing import Any

import structlog
from starlette.requests import Request

from dagster_codekit import DeploymentEvent
from dagster_codekit.core.exceptions import AuthenticationError, ConfigurationError
from dagster_codekit.core.interfaces import BackendPlugin
from dagster_codekit.utils.health import wait_for_grpc_server

logger = structlog.get_logger()


class ForgejoBackend(BackendPlugin):

    def __init__(self, config):
        from dagster_codekit.config import ForgejoBackendConfig

        self.config: ForgejoBackendConfig = config

    def name(self) -> str:
        return "forgejo"

    def validate_signature(self, request: Request) -> bool:
        secret = self.config.webhook_secret
        if not secret:
            raise ConfigurationError("Forgejo backend enabled but no webhook_secret configured")

        received = request.headers.get("X-Forgejo-Webhook-Secret", "")
        if not received:
            raise AuthenticationError("Missing X-Forgejo-Webhook-Secret header")

        if not hmac.compare_digest(secret, received):
            raise AuthenticationError("Invalid Forgejo webhook secret")

        return True

    async def parse_event(self, payload: dict[str, Any]) -> DeploymentEvent | None:
        if self.config.payload_mode == "custom":
            return self._parse_custom_payload(payload)
        return self._parse_native_payload(payload)

    def _parse_custom_payload(self, payload: dict[str, Any]) -> DeploymentEvent | None:
        location_name = payload.get("location_name")
        image_tag = payload.get("image_tag")

        if not location_name:
            raise ConfigurationError(
                "Custom Forgejo payload missing required field 'location_name'"
            )
        if not image_tag:
            raise ConfigurationError(
                "Custom Forgejo payload missing required field 'image_tag'"
            )

        return DeploymentEvent(
            location_name=location_name,
            image_tag=image_tag,
            commit_hash=payload.get("commit_hash"),
            k8s_config=payload.get("k8s_config", {}),
            metadata={
                "source": "forgejo",
                "repository": payload.get("repository", ""),
                "forgejo_actor": payload.get("actor", ""),
            },
        )

    def _parse_native_payload(self, payload: dict[str, Any]) -> DeploymentEvent | None:
        action = payload.get("action", "")
        if action not in ("published", "completed", "created"):
            logger.debug("forgejo_native_ignored_action", action=action)
            return None

        repo = payload.get("repository", {})
        repo_full_name = repo.get("full_name", "")

        package = payload.get("package", {})
        location_name = (
            package.get("name")
            or repo.get("name")
            or payload.get("location_name")
        )
        if not location_name:
            raise ConfigurationError(
                "Forgejo native payload missing location name. "
                "Set package name or add 'location_name' to the payload."
            )

        image_tag = (
            f"{repo_full_name}:{payload.get('release', {}).get('tag_name', 'latest')}"
            if payload.get("release")
            else f"{repo_full_name}:latest"
        )

        return DeploymentEvent(
            location_name=location_name,
            image_tag=image_tag,
            commit_hash=repo.get("default_branch"),
            metadata={
                "source": "forgejo",
                "repository": repo_full_name,
                "forgejo_action": action,
            },
        )

    async def wait_ready(self, event: DeploymentEvent) -> bool:
        return await wait_for_grpc_server(
            host=event.metadata.get("grpc_host", ""),
            port=event.metadata.get("grpc_port", 4000),
            timeout=self.config.grpc_timeout,
            use_tls=self.config.grpc_tls,
            check_interval=self.config.check_interval,
        )
