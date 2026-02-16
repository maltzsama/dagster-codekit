"""
Core interfaces for dagster-codekit backends.
"""

from abc import ABC, abstractmethod
from typing import Any, Optional

from starlette.requests import Request

from dagster_codekit import DeploymentEvent


class BackendPlugin(ABC):
    """
    Abstract base class for CI/CD backend plugins.
    Responsible for parsing webhooks and detecting deployments.
    """

    def __init__(self, config: dict[str, Any]):
        self.config = config

    @abstractmethod
    def name(self) -> str:
        """Return unique name for this backend (e.g., 'argocd', 'github')."""
        pass

    @abstractmethod
    def validate_signature(self, request: Request) -> bool:
        """
        Validate webhook signature using HMAC or token.
        Raises AuthenticationError if invalid.
        """
        pass

    @abstractmethod
    async def parse_event(self, payload: dict[str, Any]) -> Optional[DeploymentEvent]:
        """
        Parse webhook payload into a standardized DeploymentEvent.
        Returns None if the event should be ignored.
        """
        pass

    @abstractmethod
    async def wait_ready(self, event: DeploymentEvent) -> bool:
        """
        Wait for the deployment to become healthy (e.g., gRPC check).
        Returns True if ready, False if timeout.
        """
        pass
