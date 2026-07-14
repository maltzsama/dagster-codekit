"""Azure DevOps CI/CD webhook backend.

Supports Azure DevOps Service Hooks for both:
- Classic Release pipelines: ms.vss-release.deployment-completed-event
- YAML Pipelines: ms.vss-pipelines.run-state-changed-event

Authentication: Azure DevOps Service Hooks do NOT support HMAC body signing.
Instead, configure a custom HTTP header on the Service Hook subscription
(e.g., X-Codekit-Webhook-Token with a shared secret). This backend compares
that header value using constant-time comparison.

Required pipeline variables (set in your Azure DevOps pipeline/release):
- dagster.location_name: the CodeKit location name
- dagster.image_tag: the Docker image tag for the deployment
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


class AzureDevOpsBackend(BackendPlugin):

    def __init__(self, config):
        from dagster_codekit.config import AzureDevOpsBackendConfig

        self.config: AzureDevOpsBackendConfig = config

    def name(self) -> str:
        return "azure_devops"

    def validate_signature(self, request: Request) -> bool:
        """Validate the configured header token.

        Azure DevOps does not provide HMAC body signing for Service Hooks.
        Instead, a static token is placed in a custom header configured
        on the Service Hook subscription.
        """
        secret = self.config.webhook_secret
        if not secret:
            raise ConfigurationError(
                "Azure DevOps backend enabled but no webhook_secret configured"
            )

        header_name = self.config.header_name
        received = request.headers.get(header_name, "")

        if not received:
            raise AuthenticationError(f"Missing '{header_name}' header")

        if not hmac.compare_digest(secret, received):
            raise AuthenticationError("Invalid Azure DevOps webhook token")

        return True

    async def parse_event(self, payload: dict[str, Any]) -> DeploymentEvent | None:
        event_type = payload.get("eventType", "")
        resource = payload.get("resource", {})

        if event_type == "ms.vss-release.deployment-completed-event":
            return self._parse_classic_release(resource)
        elif event_type == "ms.vss-pipelines.run-state-changed-event":
            return self._parse_yaml_pipeline(resource)
        else:
            logger.debug("azure_devops_unknown_event", event_type=event_type)
            return None

    def _parse_classic_release(self, resource: dict[str, Any]) -> DeploymentEvent | None:
        environment = resource.get("environment", {})
        deployment = resource.get("deployment", {})
        release = resource.get("release", {})
        project = resource.get("project", {})

        status = deployment.get("deploymentStatus", "")
        if status != "succeeded":
            logger.debug("azure_devops_ignored_status", status=status)
            return None

        variables = release.get("variables", {})
        location_name = self._get_variable(variables, "dagster.location_name")
        if not location_name:
            raise ConfigurationError(
                "Azure DevOps Classic Release missing required variable "
                "'dagster.location_name'. Add it to your release pipeline variables."
            )

        image_tag = self._get_variable(variables, "dagster.image_tag")
        if not image_tag:
            raise ConfigurationError(
                "Azure DevOps Classic Release missing required variable "
                "'dagster.image_tag'. Add it to your release pipeline variables."
            )

        return DeploymentEvent(
            location_name=location_name,
            image_tag=image_tag,
            commit_hash=self._get_variable(variables, "dagster.commit_hash"),
            k8s_config=self._parse_k8s_variables(variables),
            metadata={
                "source": "azure_devops",
                "pipeline_type": "classic_release",
                "release_name": release.get("name", ""),
                "project": project.get("name", ""),
                "environment": environment.get("name", ""),
            },
        )

    def _parse_yaml_pipeline(self, resource: dict[str, Any]) -> DeploymentEvent | None:
        run = resource.get("run", resource)
        pipeline = resource.get("pipeline", {})
        project = resource.get("project", {})

        result = run.get("result", "")
        if result != "succeeded":
            logger.debug("azure_devops_ignored_result", result=result)
            return None

        variables = run.get("variables", {})
        if not variables and "templateParameters" in run:
            variables = run.get("templateParameters", {})

        location_name = self._get_variable(variables, "dagster.location_name")
        if not location_name:
            raise ConfigurationError(
                "Azure DevOps YAML Pipeline missing required variable "
                "'dagster.location_name'. Add it to your pipeline variables."
            )

        image_tag = self._get_variable(variables, "dagster.image_tag")
        if not image_tag:
            raise ConfigurationError(
                "Azure DevOps YAML Pipeline missing required variable "
                "'dagster.image_tag'. Add it to your pipeline variables."
            )

        return DeploymentEvent(
            location_name=location_name,
            image_tag=image_tag,
            commit_hash=self._get_variable(variables, "dagster.commit_hash"),
            k8s_config=self._parse_k8s_variables(variables),
            metadata={
                "source": "azure_devops",
                "pipeline_type": "yaml_pipeline",
                "pipeline_name": pipeline.get("name", ""),
                "project": project.get("name", ""),
                "run_name": run.get("name", ""),
            },
        )

    def _get_variable(self, variables: dict[str, Any], name: str) -> str | None:
        var = variables.get(name, {})
        if isinstance(var, dict):
            return var.get("value") or var.get("Value")
        return var if isinstance(var, str) else None

    def _parse_k8s_variables(self, variables: dict[str, Any]) -> dict[str, Any]:
        config = {}
        for key, val in variables.items():
            if key.startswith("dagster.k8s."):
                actual_key = key[len("dagster.k8s."):]
                actual_val = val.get("value", val) if isinstance(val, dict) else val
                config[actual_key] = actual_val
        return config

    async def wait_ready(self, event: DeploymentEvent) -> bool:
        return await wait_for_grpc_server(
            host=event.metadata.get("grpc_host", ""),
            port=event.metadata.get("grpc_port", 4000),
            timeout=self.config.grpc_timeout,
            use_tls=self.config.grpc_tls,
            check_interval=self.config.check_interval,
        )
