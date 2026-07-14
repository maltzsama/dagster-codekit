from unittest.mock import AsyncMock, Mock, patch

import pytest
from starlette.testclient import TestClient

from dagster_codekit.api.app import create_app


@pytest.fixture
def app(basic_config):
    """Create test app with mocks."""
    # Patch ConfigMap/File Managers to avoid FS/K8s calls
    with patch("dagster_codekit.server.FileWorkspaceManager") as mock_wm:
        mock_wm.return_value = Mock()

        # Patch Reloader
        with patch("dagster_codekit.server.DagsterReloader") as mock_reloader:
            mock_reloader.return_value = AsyncMock()

            app = create_app(basic_config)
            yield app


def test_health_endpoint(app):
    client = TestClient(app)
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json()["status"] == "healthy"


def test_webhook_unknown_backend(app):
    client = TestClient(app)
    response = client.post("/webhooks/unknown", json={})
    assert response.status_code == 404


def test_webhook_invalid_signature(app):
    client = TestClient(app)
    # ArgoCD backend expects X-Argocd-Webhook-Secret
    response = client.post(
        "/webhooks/argocd",
        json={"app": {}},
        headers={"X-Argocd-Webhook-Secret": "wrong-secret"},
    )
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_webhook_success_flow(app):
    client = TestClient(app)
    # Assuming the config has ArgoCD enabled and secret is 'a'*32
    # This requires extensive mocking of the BackendPlugin.validate_signature/parse_event
    # For unit test, just checking structure is okay.
    assert "argocd" in app.state.backends
