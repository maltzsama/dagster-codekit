"""Tests for configuration validation."""

import tempfile
from pathlib import Path

import pytest
from pydantic import ValidationError

from dagster_codekit.config import (
    ArgoCDBackendConfig,
    AuthConfig,
    Config,
    DagsterConfig,
    GitHubBackendConfig,
    LocationConfig,
    RepositoryConfig,
    ServerConfig,
    WorkspaceConfig,
    load_config,
)

# LocationConfig Tests


def test_location_config_valid():
    """Test valid LocationConfig."""
    loc = LocationConfig(name="analytics", grpc_host="analytics.dagster.svc", grpc_port=4000)

    assert loc.name == "analytics"
    assert loc.grpc_host == "analytics.dagster.svc"
    assert loc.grpc_port == 4000
    assert loc.path is None


def test_location_config_with_path():
    """Test LocationConfig with path (monorepo)."""
    loc = LocationConfig(
        name="analytics",
        grpc_host="analytics.svc",
        grpc_port=4000,
        path="analytics/",
    )

    assert loc.path == "analytics/"


def test_location_config_empty_name():
    """Test that empty name raises error."""
    with pytest.raises(ValidationError, match="location name cannot be empty"):
        LocationConfig(name="", grpc_host="test.svc", grpc_port=4000)


def test_location_config_name_with_spaces():
    """Test that name with spaces raises error."""
    with pytest.raises(ValidationError, match="contains spaces"):
        LocationConfig(name="my location", grpc_host="test.svc", grpc_port=4000)


def test_location_config_empty_grpc_host():
    """Test that empty grpc_host raises error."""
    with pytest.raises(ValidationError, match="grpc_host cannot be empty"):
        LocationConfig(name="test", grpc_host="", grpc_port=4000)


def test_location_config_grpc_host_with_protocol():
    """Test that grpc_host with protocol raises error."""
    with pytest.raises(ValidationError, match="should not include protocol"):
        LocationConfig(name="test", grpc_host="http://test.svc", grpc_port=4000)


def test_location_config_invalid_port():
    """Test that invalid port raises error."""
    with pytest.raises(ValidationError):
        LocationConfig(name="test", grpc_host="test.svc", grpc_port=0)

    with pytest.raises(ValidationError):
        LocationConfig(name="test", grpc_host="test.svc", grpc_port=99999)


# RepositoryConfig Tests


def test_repository_config_valid():
    """Test valid RepositoryConfig."""
    repo = RepositoryConfig(
        repo="company/data-platform",
        locations=[LocationConfig(name="analytics", grpc_host="analytics.svc", grpc_port=4000)],
    )

    assert repo.repo == "company/data-platform"
    assert len(repo.locations) == 1


def test_repository_config_invalid_format():
    """Test that invalid repo format raises error."""
    with pytest.raises(ValidationError, match="Invalid repo format"):
        RepositoryConfig(
            repo="invalid-repo-name",
            locations=[LocationConfig(name="test", grpc_host="test.svc", grpc_port=4000)],
        )


def test_repository_config_duplicate_names():
    """Test that duplicate location names raises error."""
    with pytest.raises(ValidationError, match="Duplicate location names"):
        RepositoryConfig(
            repo="company/repo",
            locations=[
                LocationConfig(name="analytics", grpc_host="a.svc", grpc_port=4000),
                LocationConfig(name="analytics", grpc_host="b.svc", grpc_port=4000),
            ],
        )


def test_repository_config_empty_locations():
    """Test that empty locations list raises error."""
    with pytest.raises(ValidationError):
        RepositoryConfig(repo="company/repo", locations=[])


# AuthConfig Tests


def test_auth_config_none():
    """Test auth type 'none' (default)."""
    auth = AuthConfig(type="none")
    assert auth.type == "none"


def test_auth_config_header_valid():
    """Test valid header auth."""
    auth = AuthConfig(type="header", header_name="X-Auth-Token", token_env="TOKEN")

    assert auth.type == "header"
    assert auth.header_name == "X-Auth-Token"
    assert auth.token_env == "TOKEN"


def test_auth_config_header_missing_header_name():
    """Test that header auth without header_name raises error."""
    with pytest.raises(ValidationError, match="requires 'header_name'"):
        AuthConfig(type="header", token_env="TOKEN")


def test_auth_config_header_missing_token():
    """Test that header auth without token raises error."""
    with pytest.raises(ValidationError, match="requires either 'token_env' or 'token'"):
        AuthConfig(type="header", header_name="X-Auth")


def test_auth_config_basic_valid():
    """Test valid basic auth."""
    auth = AuthConfig(type="basic", username="user", password_env="PASS")

    assert auth.type == "basic"
    assert auth.username == "user"


def test_auth_config_basic_missing_username():
    """Test that basic auth without username raises error."""
    with pytest.raises(ValidationError, match="requires 'username'"):
        AuthConfig(type="basic", password_env="PASS")


# GitHubBackendConfig Tests


def test_github_backend_disabled():
    """Test disabled GitHub backend (no validation)."""
    config = GitHubBackendConfig(enabled=False, webhook_secret="dummy")

    assert config.enabled is False


def test_github_backend_enabled_without_repos():
    """Test that enabled backend without repos raises error."""
    with pytest.raises(ValidationError, match="no repositories configured"):
        GitHubBackendConfig(enabled=True, webhook_secret="x" * 32, repositories=[])


def test_github_backend_short_secret():
    """Test that short webhook_secret raises error when enabled."""
    # Short secret com enabled=True deve falhar
    with pytest.raises(ValidationError, match="too short"):
        GitHubBackendConfig(
            enabled=True,
            webhook_secret="short",
            repositories=[
                RepositoryConfig(
                    repo="test/repo",
                    locations=[LocationConfig(name="test", grpc_host="test.svc", grpc_port=4000)],
                )
            ],
        )

    config = GitHubBackendConfig(enabled=False, webhook_secret="short")
    assert config.enabled is False


def test_github_backend_valid():
    """Test valid GitHub backend config."""
    config = GitHubBackendConfig(
        enabled=True,
        webhook_secret="x" * 32,
        repositories=[
            RepositoryConfig(
                repo="company/repo",
                locations=[LocationConfig(name="test", grpc_host="test.svc", grpc_port=4000)],
            )
        ],
    )

    assert config.enabled is True
    assert len(config.repositories) == 1


# Full Config Tests


def test_config_minimal_valid():
    """Test minimal valid configuration."""
    config_dict = {
        "dagster": {"webserver_url": "http://dagster:3000"},
        "workspace": {
            "mode": "file",
            "file": {"path": "/opt/dagster/workspace.yaml"},
        },
        "backends": {"argocd": {"enabled": True, "webhook_secret": "x" * 32, "grpc_timeout": 60}},
    }

    config = Config(**config_dict)

    assert config.dagster.webserver_url == "http://dagster:3000"
    assert config.workspace.mode == "file"
    assert config.server.port == 8000  # Default


def test_config_no_backends_enabled():
    """Test that config with no enabled backends raises error."""
    config_dict = {
        "dagster": {"webserver_url": "http://dagster:3000"},
        "workspace": {"mode": "file", "file": {"path": "/workspace.yaml"}},
        "backends": {"argocd": {"enabled": False, "webhook_secret": "x" * 32}},
    }

    with pytest.raises(ValidationError, match="No backends enabled"):
        Config(**config_dict)


def test_config_invalid_webserver_url():
    """Test that invalid webserver URL raises error."""
    config_dict = {
        "dagster": {"webserver_url": "not-a-url"},
        "workspace": {"mode": "file", "file": {"path": "/workspace.yaml"}},
        "backends": {"argocd": {"enabled": True, "webhook_secret": "x" * 32}},
    }

    with pytest.raises(ValidationError, match="Must start with http"):
        Config(**config_dict)


def test_config_workspace_file_missing_config():
    """Test that file mode without file config raises error."""
    config_dict = {
        "dagster": {"webserver_url": "http://dagster:3000"},
        "workspace": {"mode": "file"},
        "backends": {"argocd": {"enabled": True, "webhook_secret": "x" * 32}},
    }

    with pytest.raises(ValidationError, match="workspace.file not configured"):
        Config(**config_dict)


# load_config Tests


def test_load_config_file_not_found():
    """Test that missing config file exits with code 1."""
    with pytest.raises(SystemExit) as exc_info:
        load_config("/nonexistent/config.yaml")

    assert exc_info.value.code == 1


def test_load_config_invalid_yaml():
    """Test that invalid YAML exits with code 1."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        f.write("invalid: yaml: content:\n  - broken")
        f.flush()

        try:
            with pytest.raises(SystemExit) as exc_info:
                load_config(f.name)

            assert exc_info.value.code == 1
        finally:
            Path(f.name).unlink()


def test_load_config_valid():
    """Test loading valid config file."""
    config_yaml = """
server:
  port: 8000

dagster:
  webserver_url: http://dagster-webserver:3000

workspace:
  mode: file
  file:
    path: /opt/dagster/workspace.yaml

backends:
  argocd:
    enabled: true
    webhook_secret: xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
    grpc_timeout: 60
"""

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        f.write(config_yaml)
        f.flush()

        try:
            config = load_config(f.name)

            assert config.server.port == 8000
            assert config.dagster.webserver_url == "http://dagster-webserver:3000"
            assert config.workspace.mode == "file"
        finally:
            Path(f.name).unlink()
