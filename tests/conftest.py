import pytest
from dagster_codekit.config import Config, DagsterConfig, WorkspaceConfig, ServerConfig


@pytest.fixture
def basic_config():
    return Config(
        server=ServerConfig(),
        dagster=DagsterConfig(webserver_url="http://localhost:3000"),
        workspace=WorkspaceConfig(mode="file", file={"path": "/tmp/workspace.yaml"}),
        backends={"argocd": {"enabled": True, "webhook_secret": "a" * 32}},  # 32 chars fake secret
    )
