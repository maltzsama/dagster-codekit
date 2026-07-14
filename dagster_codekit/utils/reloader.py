"""
Dagster Webserver reloader.
Triggers workspace reload via GraphQL mutation using typed configuration.
"""

import base64
import os
from typing import Any

import httpx
import structlog

from dagster_codekit.config import AuthConfig
from dagster_codekit.core.exceptions import (
    AuthenticationError,
    CodekitError,
    ConfigurationError,
    ConnectionError,
    TimeoutError,
)

# Global logger using contextvars for tracing (location, request_id, etc.)
logger = structlog.get_logger()


class DagsterReloader:
    """
    Handles triggering code location reloads on the Dagster Webserver.

    This implementation focuses on Dagster Open Source (OSS) environments,
    supporting standard authentication methods like Header, Bearer, and Basic Auth.
    """

    def __init__(self, webserver_url: str, auth_config: AuthConfig):
        """
        Initialize the reloader with a typed configuration.

        Args:
            webserver_url: The base URL of the Dagster Webserver.
            auth_config: An instance of AuthConfig validated by Pydantic.
        """
        self.webserver_url = webserver_url.rstrip("/")
        self.graphql_url = f"{self.webserver_url}/graphql"
        self.auth = auth_config

        logger.info(
            "reloader_initialized",
            webserver_url=self.webserver_url,
            auth_type=self.auth.type,
        )

    def _get_auth_value(self, env_field: str, direct_field: str) -> str:
        """
        Extract auth values from environment variables or direct strings.

        Args:
            env_field: The name of the field in AuthConfig containing the ENV var name.
            direct_field: The name of the field in AuthConfig containing the raw value.
        """
        env_var_name = getattr(self.auth, env_field)
        direct_value = getattr(self.auth, direct_field)

        if env_var_name:
            value = os.getenv(env_var_name)
            if not value:
                raise ConfigurationError(
                    f"Environment variable '{env_var_name}' not set (from auth.{env_field})"
                )
            return value

        if direct_value:
            return direct_value

        raise ConfigurationError(
            f"Authentication requires either '{env_field}' or '{direct_field}'"
        )

    def _build_auth_headers(self) -> dict[str, str]:
        """
        Construct the required HTTP headers based on the authentication type.
        """
        headers = {"Content-Type": "application/json"}

        if self.auth.type == "none":
            return headers

        if self.auth.type == "header":
            # Pydantic ensures header_name exists if type is 'header'
            token = self._get_auth_value("token_env", "token")
            headers[self.auth.header_name] = token

        elif self.auth.type == "bearer":
            token = self._get_auth_value("token_env", "token")
            headers["Authorization"] = f"Bearer {token}"

        elif self.auth.type == "basic":
            password = self._get_auth_value("password_env", "password")
            credentials = f"{self.auth.username}:{password}"
            encoded = base64.b64encode(credentials.encode()).decode()
            headers["Authorization"] = f"Basic {encoded}"

        return headers

    async def reload(self) -> None:
        """
        Trigger a workspace reload by sending a GraphQL mutation.

        Raises:
            AuthenticationError: On 401/403 responses.
            ConfigurationError: On 404 or endpoint misconfiguration.
            TimeoutError: If the server takes longer than 30s.
            ConnectionError: If the webserver is unreachable.
        """
        mutation = {"query": "mutation ReloadWorkspace { reloadWorkspace { __typename } }"}

        headers = self._build_auth_headers()

        logger.info("reloader_triggering_reload", url=self.graphql_url)

        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.post(
                    self.graphql_url,
                    json=mutation,
                    headers=headers,
                )

                self._handle_http_errors(response)

                data = response.json()
                if "errors" in data:
                    self._handle_graphql_errors(data["errors"])

                logger.info("reloader_workspace_reloaded", status="success")

        except httpx.ConnectError as e:
            raise ConnectionError(f"Could not connect to Dagster at {self.webserver_url}: {e}")
        except httpx.TimeoutException:
            raise TimeoutError("Request to Dagster timed out after 30 seconds")
        except Exception as e:
            # Re-raise known exceptions, log and raise unknown ones
            if isinstance(
                e, (AuthenticationError, ConfigurationError, ConnectionError, TimeoutError)
            ):
                raise
            logger.error("reloader_unexpected_error", error=str(e), exc_info=True)
            raise e

    async def check_connection(self) -> bool:
        """
        Check if Dagster Webserver is reachable.
        """
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                response = await client.get(f"{self.webserver_url}/server_info")
                return response.status_code == 200
        except Exception as e:
            logger.warning("reloader_connection_check_failed", error=str(e))
            return False

    def _handle_http_errors(self, response: httpx.Response) -> None:
        """Process HTTP status codes and raise specific exceptions."""
        if response.status_code == 200:
            return

        if response.status_code in (401, 403):
            raise AuthenticationError(
                f"Dagster authentication failed (HTTP {response.status_code}). "
                "Verify your authentication credentials in config.yaml."
            )

        if response.status_code == 404:
            raise ConfigurationError(
                f"Dagster GraphQL endpoint not found. Check if the webserver URL "
                f"is correct: {self.webserver_url}"
            )

        raise CodekitError(
            f"Dagster returned an unexpected HTTP {response.status_code}: {response.text}"
        )

    def _handle_graphql_errors(self, errors: list[dict[str, Any]]) -> None:
        """Process errors returned within the GraphQL response body."""
        messages = [error.get("message", "Unknown GraphQL error") for error in errors]
        error_summary = " | ".join(messages)

        logger.error("reloader_graphql_errors", errors=messages)
        raise CodekitError(f"Dagster GraphQL execution failed: {error_summary}")
