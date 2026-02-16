import pytest
from unittest.mock import AsyncMock, MagicMock
from dagster_codekit.engine import DeploymentEngine
from dagster_codekit.models import DeploymentEvent
from dagster_codekit.workspace.base import WorkspaceManager
from dagster_codekit.utils.reloader import DagsterReloader


@pytest.mark.asyncio
async def test_engine_process_deployment_success():
    # Mocks
    mock_workspace = MagicMock(spec=WorkspaceManager)
    mock_reloader = AsyncMock(spec=DagsterReloader)

    engine = DeploymentEngine(workspace_manager=mock_workspace, reloader=mock_reloader)

    event = DeploymentEvent(
        location_name="analytics",
        grpc_host="analytics.svc",
        grpc_port=4000,
        deployment_type="grpc-service",
    )

    # Act
    await engine.process_deployment(event)

    # Assert
    mock_workspace.add_or_update.assert_called_once()
    mock_reloader.reload.assert_awaited_once()

    # Verify logic of config builder
    call_args = mock_workspace.add_or_update.call_args
    assert call_args[0][0] == "analytics"
    assert call_args[0][1] == {
        "grpc_server": {"host": "analytics.svc", "port": 4000, "location_name": "analytics"}
    }
