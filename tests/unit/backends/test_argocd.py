from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dagster_codekit.backends.argocd import ArgoCDBackend
from dagster_codekit.config import ArgoCDBackendConfig
from dagster_codekit.core.exceptions import AuthenticationError


@pytest.fixture
def argocd_backend():
    cfg = ArgoCDBackendConfig(enabled=True, webhook_secret="secret" * 5, grpc_timeout=60)
    return ArgoCDBackend(config=cfg)


def test_signature_validation(argocd_backend):
    request = MagicMock()
    request.headers = {"X-Argocd-Webhook-Secret": "secret" * 5}

    assert argocd_backend.validate_signature(request) is True


def test_signature_validation_fail(argocd_backend):
    request = MagicMock()
    request.headers = {"X-Argocd-Webhook-Secret": "wrong"}

    with pytest.raises(AuthenticationError):
        argocd_backend.validate_signature(request)


@pytest.mark.asyncio
async def test_parse_event_success(argocd_backend):
    payload = {
        "app": {
            "metadata": {
                "name": "my-app",
                "namespace": "dagster",
                "labels": {
                    "dagster.io/code-location": "true",
                    "dagster.io/location-name": "analytics",
                },
            },
            "status": {"sync": {"status": "Synced"}, "health": {"status": "Healthy"}},
        }
    }

    event = await argocd_backend.parse_event(payload)

    assert event is not None
    assert event.location_name == "analytics"
    assert event.grpc_host == "my-app.dagster.svc.cluster.local"  # Default convention
    assert event.grpc_port == 4000


@pytest.mark.asyncio
async def test_wait_ready_calls_util(argocd_backend):
    # Mock the utility function to avoid actual network calls
    with patch(
        "dagster_codekit.backends.argocd.wait_for_grpc_server", new_callable=AsyncMock
    ) as mock_wait:
        mock_wait.return_value = True

        event = MagicMock()
        event.grpc_host = "host"
        event.grpc_port = 1234
        event.location_name = "loc"

        result = await argocd_backend.wait_ready(event)

        assert result is True
        mock_wait.assert_awaited_once()
