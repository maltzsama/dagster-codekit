"""Tests for CLI commands."""

import tempfile
from pathlib import Path
from unittest.mock import AsyncMock, patch  # <--- Importante: AsyncMock

from click.testing import CliRunner

from dagster_codekit.cli import main


def test_cli_version():
    """Test version command."""
    runner = CliRunner()
    result = runner.invoke(cli, ["version"])

    assert result.exit_code == 0
    assert "dagster-codekit version" in result.output
    assert "0.1.0" in result.output


def test_cli_help():
    """Test help command."""
    runner = CliRunner()
    result = runner.invoke(main, ["--help"])

    assert result.exit_code == 0
    assert "dagster-codekit" in result.output


def test_cli_init():
    """Test init command generates example config."""
    runner = CliRunner()
    result = runner.invoke(cli, ["init"])

    assert result.exit_code == 0
    assert "server:" in result.output


def test_cli_validate_missing_file():
    """Test validate command with missing file."""
    runner = CliRunner()
    result = runner.invoke(main, ["validate", "/nonexistent/config.yaml"])
    assert result.exit_code == 1


def test_cli_validate_invalid_yaml():
    """Test validate command with invalid YAML."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        f.write("invalid: yaml: content:\n  - broken")
        f.flush()
        try:
            runner = CliRunner()
            result = runner.invoke(cli, ["validate", f.name])
            assert result.exit_code == 1
        finally:
            Path(f.name).unlink()


def test_cli_validate_valid_config():
    """Test validate command with valid config."""
    config_yaml = """
server:
  port: 8000
dagster:
  webserver_url: http://dagster-webserver:3000
workspace:
  mode: file
  file:
    path: /tmp/workspace.yaml
backends:
  argocd:
    enabled: true
    webhook_secret: xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
"""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        f.write(config_yaml)
        f.flush()
        try:
            runner = CliRunner()
            result = runner.invoke(cli, ["validate", f.name])
            assert result.exit_code == 0
            assert "Config is valid" in result.output
        finally:
            Path(f.name).unlink()


def test_cli_start_missing_config():
    """Test start command with missing config file."""
    runner = CliRunner()
    result = runner.invoke(cli, ["start", "--config", "/nonexistent/config.yaml"])
    assert result.exit_code != 0


def test_cli_start_with_valid_config():
    """Test start command. Mocks run_server to avoid actual network/loop."""

    # 1. Cria um workspace.yaml REAL temporário
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as ws:
        ws.write("load_from: []")
        ws.flush()
        workspace_path = ws.name

    # 2. Cria config apontando para esse workspace
    config_yaml = f"""
server:
  host: 0.0.0.0
  port: 8000
dagster:
  webserver_url: http://dagster-webserver:3000
workspace:
  mode: file
  file:
    path: {workspace_path}
backends:
  argocd:
    enabled: true
    webhook_secret: xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
"""

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as cfg:
        cfg.write(config_yaml)
        cfg.flush()
        config_path = cfg.name

        try:
            # CORREÇÃO: Usamos AsyncMock.
            # Ao ser chamado, ele retorna uma corrotina automaticamente,
            # satisfazendo o asyncio.run() do CLI.
            with patch("dagster_codekit.server.run_server", new_callable=AsyncMock) as mock_run:

                runner = CliRunner()
                result = runner.invoke(main, ["start", "--config", config_path])

                assert result.exit_code == 0
                assert "Starting dagster-codekit" in result.output
                assert "Config validated successfully" in result.output

                mock_run.assert_called_once()

        finally:
            Path(config_path).unlink(missing_ok=True)
            Path(workspace_path).unlink(missing_ok=True)


def test_cli_start_with_port_override():
    """Test start command with port override."""

    # 1. Workspace
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as ws:
        ws.write("load_from: []")
        ws.flush()
        workspace_path = ws.name

    # 2. Config
    config_yaml = f"""
server:
  port: 8000
dagster:
  webserver_url: http://dagster:3000
workspace:
  mode: file
  file:
    path: {workspace_path}
backends:
  argocd:
    enabled: true
    webhook_secret: xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
"""

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as cfg:
        cfg.write(config_yaml)
        cfg.flush()
        config_path = cfg.name

        try:
            # CORREÇÃO: AsyncMock aqui também
            with patch("dagster_codekit.server.run_server", new_callable=AsyncMock) as mock_run:

                runner = CliRunner()
                result = runner.invoke(cli, ["start", "--config", config_path, "--port", "9000"])

                assert result.exit_code == 0

                # Verifica se a config passada pro server tem a porta 9000
                args, _ = mock_run.call_args
                passed_config = args[0]
                assert passed_config.server.port == 9000

        finally:
            Path(config_path).unlink(missing_ok=True)
            Path(workspace_path).unlink(missing_ok=True)


def test_cli_validate_shows_summary():
    """Test that validate command shows configuration summary."""
    config_yaml = """
server:
  port: 9999
dagster:
  webserver_url: http://test-dagster:3000
workspace:
  mode: configmap
  configmap:
    namespace: test-namespace
    name: test-workspace
backends:
  argocd:
    enabled: true
    webhook_secret: xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
"""

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        f.write(config_yaml)
        f.flush()
        try:
            runner = CliRunner()
            result = runner.invoke(cli, ["validate", f.name])
            assert result.exit_code == 0
            assert "0.0.0.0:9999" in result.output
            assert "configmap" in result.output
            assert "argocd" in result.output
        finally:
            Path(f.name).unlink()
