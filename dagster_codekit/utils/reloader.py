"""
Dagster Webserver reloader.

Triggers workspace reload via GraphQL mutation.
"""

import os
from typing import Optional

import httpx
import structlog

from dagster_codekit.exceptions import (
    AuthenticationError,
    ConfigurationError,
    ConnectionError,
    TimeoutError,
)

logger = structlog.get_logger()


class DagsterReloader:
    """
    Trigger workspace reload in Dagster Webserver.

    Uses GraphQL mutation `reloadWorkspace` available since Dagster 1.5+.

    Supports multiple authentication methods:
    - none: No authentication
    - header: Custom header (e.g., X-Auth-Token)
    - bearer: OAuth Bearer token
    - basic: HTTP Basic Auth
    - dagster_cloud: Dagster+ API token
    """

    def __init__(self, webserver_url: str, auth_config: Optional[dict] = None):
        """
        Initialize reloader.

        Args:
            webserver_url: Dagster webserver URL (e.g., http://dagster:3000)
            auth_config: Authentication configuration (optional)

        Raises:
            ConfigurationError: If auth config is invalid
        """
        self.webserver_url = webserver_url.rstrip("/")
        self.graphql_url = f"{self.webserver_url}/graphql"
        self.auth_config = auth_config or {}

        # Build auth headers
        self.headers = self._build_auth_headers()

        logger.info(
            "reloader_initialized",
            webserver_url=webserver_url,
            auth_type=self.auth_config.get("type", "none"),
        )

    def _build_auth_headers(self) -> dict:
        """
        Build authentication headers based on config.

        Returns:
            Headers dict for HTTP requests

        Raises:
            ConfigurationError: If auth config is invalid
        """
        headers = {"Content-Type": "application/json"}

        auth_type = self.auth_config.get("type", "none")

        if auth_type == "none":
            return headers

        elif auth_type == "header":
            # Custom header authentication
            header_name = self.auth_config.get("header_name")
            if not header_name:
                raise ConfigurationError("auth.type='header' requires 'header_name' field")

            # Token from env var or direct config
            token = self._get_auth_value("token_env", "token")

            headers[header_name] = token

        elif auth_type == "bearer":
            # OAuth Bearer token
            token = self._get_auth_value("token_env", "token")
            headers["Authorization"] = f"Bearer {token}"

        elif auth_type == "basic":
            # HTTP Basic Auth
            import base64

            username = self.auth_config.get("username")
            if not username:
                raise ConfigurationError("auth.type='basic' requires 'username' field")

            password = self._get_auth_value("password_env", "password")

            credentials = f"{username}:{password}"
            encoded = base64.b64encode(credentials.encode()).decode()
            headers["Authorization"] = f"Basic {encoded}"

        elif auth_type == "dagster_cloud":
            # Dagster+ API token
            token = self._get_auth_value("api_token_env", "api_token")
            headers["Dagster-Cloud-Api-Token"] = token

        else:
            raise ConfigurationError(f"Unknown auth type: {auth_type}")

        return headers

    def _get_auth_value(self, env_key: str, direct_key: str) -> str:
        """
        Get auth value from env var or direct config.

        Args:
            env_key: Config key for env var name
            direct_key: Config key for direct value

        Returns:
            Auth value

        Raises:
            ConfigurationError: If value not found
        """
        # Try env var first
        if env_key in self.auth_config:
            env_var = self.auth_config[env_key]
            value = os.getenv(env_var)
            if not value:
                raise ConfigurationError(
                    f"Environment variable '{env_var}' not set (from auth.{env_key})"
                )
            return value

        # Try direct value
        if direct_key in self.auth_config:
            return self.auth_config[direct_key]

        raise ConfigurationError(f"auth requires either '{env_key}' or '{direct_key}' field")

    async def reload(self) -> None:
        """
        Trigger workspace reload.

        Sends GraphQL mutation to Dagster Webserver.

        Raises:
            AuthenticationError: If auth fails (401, 403)
            ConfigurationError: If Dagster endpoint not found (404)
            TimeoutError: If request times out
            ConnectionError: If cannot connect to Dagster
            Exception: For other errors
        """
        mutation = """
        mutation ReloadWorkspace {
            reloadWorkspace {
                __typename
            }
        }
        """

        logger.info("reloader_triggering_reload", webserver=self.webserver_url)

        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.post(
                    self.graphql_url,
                    json={"query": mutation},
                    headers=self.headers,
                )

                # Handle HTTP errors explicitly
                if response.status_code == 401:
                    raise AuthenticationError(
                        "Dagster rejected reload request: 401 Unauthorized\n\n"
                        "Your Dagster webserver requires authentication.\n"
                        "Configure auth in config.yaml:\n\n"
                        "dagster:\n"
                        "  webserver_url: http://dagster-webserver:3000\n"
                        "  auth:\n"
                        "    type: header\n"
                        "    header_name: X-Auth-Token\n"
                        "    token_env: DAGSTER_AUTH_TOKEN\n\n"
                        "See: https://github.com/dagster-codekit/docs/authentication"
                    )

                elif response.status_code == 403:
                    raise AuthenticationError(
                        "Dagster rejected reload request: 403 Forbidden\n\n"
                        "dagster-codekit does not have permission to reload workspace.\n"
                        "If using dagster-authkit, ensure service account has "
                        "'workspace:reload' permission."
                    )

                elif response.status_code == 404:
                    raise ConfigurationError(
                        f"Dagster GraphQL endpoint not found: {self.graphql_url}\n\n"
                        f"Check webserver_url in config.yaml:\n"
                        f"  Current: {self.webserver_url}\n\n"
                        f"Expected endpoint: {self.webserver_url}/graphql\n"
                        f"Test manually: curl {self.graphql_url} "
                        f'-d \'{{"query": "{{__typename}}"}}\''
                    )

                elif response.status_code == 500:
                    raise Exception(
                        f"Dagster returned 500 Internal Server Error:\n"
                        f"{response.text}\n\n"
                        f"Check Dagster webserver logs:\n"
                        f"  kubectl logs -n dagster deployment/dagster-webserver"
                    )

                elif response.status_code != 200:
                    raise Exception(
                        f"Unexpected HTTP status {response.status_code} from Dagster:\n"
                        f"{response.text}"
                    )

                # Parse GraphQL response
                try:
                    data = response.json()
                except ValueError as e:
                    raise Exception(
                        f"Dagster returned invalid JSON:\n"
                        f"{response.text}\n\n"
                        f"Is {self.webserver_url} really a Dagster webserver?"
                    )

                # Check for GraphQL errors
                if "errors" in data:
                    errors = data["errors"]
                    error_messages = "\n".join(f"  • {e.get('message', str(e))}" for e in errors)

                    raise Exception(
                        f"Dagster GraphQL errors:\n{error_messages}\n\n"
                        f"This may indicate:\n"
                        f"  1. Dagster version too old (need 1.5+)\n"
                        f"  2. reloadWorkspace mutation not available\n"
                        f"  3. Workspace configuration error\n\n"
                        f"Check Dagster version: dagster --version"
                    )

                # Success
                logger.info("reloader_workspace_reloaded", status="success")

        except httpx.ConnectError as e:
            raise ConnectionError(
                f"Cannot connect to Dagster webserver at {self.webserver_url}\n\n"
                f"Possible causes:\n"
                f"  1. Dagster webserver is not running\n"
                f"  2. Wrong URL in config.yaml\n"
                f"  3. Network policy blocking connection\n"
                f"  4. DNS resolution failed for hostname\n\n"
                f"Test connection: curl {self.webserver_url}/server_info\n\n"
                f"Error details: {e}"
            )

        except httpx.TimeoutException:
            raise TimeoutError(
                f"Timeout connecting to Dagster webserver (30s)\n\n"
                f"Dagster webserver at {self.webserver_url} is too slow to respond.\n"
                f"Check webserver health and logs."
            )
