"""Tests for configuration validation."""

import tempfile
from pathlib import Path

import pytest
from pydantic import ValidationError

from dagster_codekit.config import (
    ArgoCDBackendConfig,
    AuthConfig,
    BackendsConfig,
    Config,
    DatabaseConfig,
    K8sConfig,
    LauncherConfig,
    ServerConfig,
    load_config,
)


class TestServerConfig:
    def test_defaults(self):
        s = ServerConfig()
        assert s.host == "0.0.0.0"
        assert s.port == 8000
        assert s.grpc_port == 4000
        assert s.workers == 4


class TestDatabaseConfig:
    def test_defaults(self):
        d = DatabaseConfig()
        assert "sqlite" in d.url


class TestK8sConfig:
    def test_defaults(self):
        k = K8sConfig()
        assert k.namespace == "dagster"
        assert k.service_account == "dagster"

    def test_forward_env_vars_default(self):
        k = K8sConfig()
        assert "DAGSTER_POSTGRES_USER" in k.forward_env_vars


class TestLauncherConfig:
    def test_defaults(self):
        lc = LauncherConfig()
        assert lc.mode == "k8s"
        assert lc.enabled is True

    def test_docker_mode(self):
        lc = LauncherConfig(mode="docker")
        assert lc.mode == "docker"
        assert lc.docker.network == "host"
        assert lc.docker.auto_remove is True


class TestArgoCDBackendConfig:
    def test_defaults(self):
        c = ArgoCDBackendConfig()
        assert c.enabled is False
        assert c.grpc_timeout == 30


class TestBackendsConfig:
    def test_defaults(self):
        b = BackendsConfig()
        assert b.argocd.enabled is False


class TestAuthConfig:
    def test_defaults(self):
        a = AuthConfig()
        assert a.enabled is False
        assert a.tokens == []

    def test_enabled_requires_tokens(self):
        with pytest.raises(ValidationError, match="enabled"):
            AuthConfig(enabled=True, tokens=[])

    def test_enabled_default_has_empty_tokens(self):
        a = AuthConfig(enabled=True)
        assert a.tokens == []

    def test_with_tokens(self):
        a = AuthConfig(enabled=True, tokens=["secret1", "secret2"])
        assert len(a.tokens) == 2


class TestConfig:
    def test_defaults(self):
        c = Config()
        assert c.server.port == 8000
        assert c.backends.argocd.enabled is False
        assert c.auth.enabled is False
        assert c.launcher.mode == "k8s"

    def test_from_dict(self):
        c = Config(server={"port": 9000})
        assert c.server.port == 9000
        assert c.server.host == "0.0.0.0"  # default


class TestLoadConfig:
    def test_missing_file_returns_defaults(self):
        c = load_config("/nonexistent/config.yaml")
        assert isinstance(c, Config)
        assert c.server.port == 8000

    def test_valid_yaml(self):
        config_yaml = """
server:
  port: 9999
  grpc_port: 5000

auth:
  enabled: false
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write(config_yaml)
            f.flush()

            try:
                c = load_config(f.name)
                assert c.server.port == 9999
                assert c.server.grpc_port == 5000
                assert c.server.host == "0.0.0.0"  # default
            finally:
                Path(f.name).unlink()

    def test_expandvars_in_auth_tokens(self):
        """Verify that ${VAR} placeholders in auth.tokens are resolved by load_config."""
        import os

        token_value = "s3cr3t-t0k3n-fr0m-3nv"
        os.environ["CODEKIT_AUTH_TOKENS"] = token_value
        try:
            config_yaml = """
auth:
  enabled: true
  tokens:
    - ${CODEKIT_AUTH_TOKENS}
"""
            with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
                f.write(config_yaml)
                f.flush()

            try:
                c = load_config(f.name)
                assert c.auth.enabled is True
                assert len(c.auth.tokens) == 1
                assert c.auth.tokens[0] == token_value
            finally:
                Path(f.name).unlink()
        finally:
            os.environ.pop("CODEKIT_AUTH_TOKENS", None)

    def test_expandvars_without_env_var_fails_expansion(self):
        """When ${VAR} references an unset var, expandvars leaves it as-is, which won't match."""
        config_yaml = """
auth:
  enabled: true
  tokens:
    - ${UNSET_VAR_SHOULD_FAIL}
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write(config_yaml)
            f.flush()

        try:
            c = load_config(f.name)
            # expandvars leaves unset vars as-is, so the token is the literal string
            assert c.auth.tokens[0] == "${UNSET_VAR_SHOULD_FAIL}"
        finally:
            Path(f.name).unlink()
