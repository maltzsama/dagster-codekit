"""Tests for the webhook route and backend registry."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from dagster_codekit.api.app import app
from dagster_codekit.backends.registry import BackendRegistry
from dagster_codekit.config import (
    ArgoCDBackendConfig,
    AzureDevOpsBackendConfig,
    BackendsConfig,
    ForgejoBackendConfig,
)
from dagster_codekit.core.exceptions import AuthenticationError


@pytest.fixture
def mock_registry():
    backends = BackendsConfig(
        argocd=ArgoCDBackendConfig(enabled=True, webhook_secret="secret123"),
        forgejo=ForgejoBackendConfig(
            enabled=True, webhook_secret="forgejo-secret", payload_mode="custom"
        ),
        azure_devops=AzureDevOpsBackendConfig(
            enabled=True, webhook_secret="ado-token", header_name="X-Codekit-Webhook-Token"
        ),
    )
    return BackendRegistry(backends)


class TestBackendRegistry:
    def test_get_enabled(self, mock_registry):
        assert mock_registry.get("argocd") is not None
        assert mock_registry.get("forgejo") is not None
        assert mock_registry.get("azure_devops") is not None
        assert mock_registry.get("nonexistent") is None

    def test_disabled_backend_not_registered(self):
        backends = BackendsConfig(
            argocd=ArgoCDBackendConfig(enabled=False, webhook_secret="secret123"),
        )
        registry = BackendRegistry(backends)
        assert registry.get("argocd") is None

    def test_names(self, mock_registry):
        names = mock_registry.names
        assert "argocd" in names
        assert "forgejo" in names
        assert "azure_devops" in names


class TestWebhookRoute:
    @pytest.fixture(autouse=True)
    def _patch_registry(self, mock_registry):
        with patch("dagster_codekit.api.app.backend_registry", mock_registry):
            yield

    def test_unknown_backend_returns_404(self):
        client = TestClient(app)
        response = client.post("/webhooks/unknown", json={})
        assert response.status_code == 404

    def test_argocd_bad_signature(self, mock_registry):
        backend = mock_registry.get("argocd")
        backend.validate_signature = MagicMock(
            side_effect=AuthenticationError("Invalid secret")
        )

        client = TestClient(app)
        response = client.post(
            "/webhooks/argocd",
            json={"app": {"metadata": {"name": "test"}}},
        )
        assert response.status_code == 401

    def test_argocd_event_ignored_returns_202(self, mock_registry):
        backend = mock_registry.get("argocd")
        backend.validate_signature = MagicMock(return_value=True)
        backend.parse_event = AsyncMock(return_value=None)

        client = TestClient(app)
        response = client.post(
            "/webhooks/argocd",
            json={"app": {"metadata": {"name": "test"}}},
        )
        assert response.status_code == 200
        assert response.json()["status"] == "ignored"

    def test_forgejo_custom_payload_success(self, mock_registry):
        backend = mock_registry.get("forgejo")
        backend.validate_signature = MagicMock(return_value=True)

        from dagster_codekit import DeploymentEvent

        backend.parse_event = AsyncMock(
            return_value=DeploymentEvent(
                location_name="forgejo-test",
                image_tag="registry.io/forgejo-test:v1",
            )
        )

        with patch("dagster_codekit.api.app._register_deployment") as mock_register:
            client = TestClient(app)
            response = client.post(
                "/webhooks/forgejo",
                json={
                    "location_name": "forgejo-test",
                    "image_tag": "registry.io/forgejo-test:v1",
                },
            )
            assert response.status_code == 200
            assert response.json()["status"] == "success"
            mock_register.assert_called_once()

    def test_azure_devops_bad_token(self, mock_registry):
        backend = mock_registry.get("azure_devops")
        backend.validate_signature = MagicMock(
            side_effect=AuthenticationError("Invalid token")
        )

        client = TestClient(app)
        response = client.post("/webhooks/azure_devops", json={})
        assert response.status_code == 401


class TestForgejoBackend:
    def test_parse_custom_payload(self):
        from dagster_codekit.backends.forgejo import ForgejoBackend

        backend = ForgejoBackend(
            ForgejoBackendConfig(enabled=True, webhook_secret="test", payload_mode="custom")
        )
        import asyncio

        event = asyncio.run(
            backend.parse_event({
                "location_name": "my-pipeline",
                "image_tag": "reg.io/img:v1",
                "commit_hash": "abc123",
            })
        )
        assert event is not None
        assert event.location_name == "my-pipeline"
        assert event.image_tag == "reg.io/img:v1"
        assert event.commit_hash == "abc123"
        assert event.metadata["source"] == "forgejo"

    def test_parse_custom_missing_location(self):
        from dagster_codekit.backends.forgejo import ForgejoBackend
        from dagster_codekit.core.exceptions import ConfigurationError

        backend = ForgejoBackend(
            ForgejoBackendConfig(enabled=True, webhook_secret="test")
        )
        import asyncio

        with pytest.raises(ConfigurationError, match="location_name"):
            asyncio.run(backend.parse_event({"image_tag": "reg.io/img:v1"}))


class TestAzureDevOpsBackend:
    def test_parse_classic_release_succeeded(self):
        from dagster_codekit.backends.azure_devops import AzureDevOpsBackend

        backend = AzureDevOpsBackend(
            AzureDevOpsBackendConfig(enabled=True, webhook_secret="test")
        )
        import asyncio

        payload = {
            "eventType": "ms.vss-release.deployment-completed-event",
            "resource": {
                "environment": {"name": "prod"},
                "deployment": {"deploymentStatus": "succeeded"},
                "release": {
                    "name": "Release-123",
                    "variables": {
                        "dagster.location_name": {"value": "ado-pipeline"},
                        "dagster.image_tag": {"value": "reg.io/img:v1"},
                    },
                },
                "project": {"name": "MyProject"},
            },
        }
        event = asyncio.run(backend.parse_event(payload))
        assert event is not None
        assert event.location_name == "ado-pipeline"
        assert event.image_tag == "reg.io/img:v1"
        assert event.metadata["pipeline_type"] == "classic_release"

    def test_parse_classic_release_failed(self):
        from dagster_codekit.backends.azure_devops import AzureDevOpsBackend

        backend = AzureDevOpsBackend(
            AzureDevOpsBackendConfig(enabled=True, webhook_secret="test")
        )
        import asyncio

        payload = {
            "eventType": "ms.vss-release.deployment-completed-event",
            "resource": {
                "deployment": {"deploymentStatus": "failed"},
                "release": {"name": "Release-123", "variables": {}},
            },
        }
        event = asyncio.run(backend.parse_event(payload))
        assert event is None

    def test_parse_yaml_pipeline_succeeded(self):
        from dagster_codekit.backends.azure_devops import AzureDevOpsBackend

        backend = AzureDevOpsBackend(
            AzureDevOpsBackendConfig(enabled=True, webhook_secret="test")
        )
        import asyncio

        payload = {
            "eventType": "ms.vss-pipelines.run-state-changed-event",
            "resource": {
                "run": {
                    "result": "succeeded",
                    "name": "20250101.1",
                    "variables": {
                        "dagster.location_name": "yaml-pipeline",
                        "dagster.image_tag": "reg.io/img:v2",
                    },
                },
                "pipeline": {"name": "My Pipeline"},
                "project": {"name": "MyProject"},
            },
        }
        event = asyncio.run(backend.parse_event(payload))
        assert event is not None
        assert event.location_name == "yaml-pipeline"
        assert event.image_tag == "reg.io/img:v2"
        assert event.metadata["pipeline_type"] == "yaml_pipeline"
