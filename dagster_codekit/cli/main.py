"""
Command-line interface for dagster-codekit.

Provides commands to start server, validate config, and generate examples.
"""

import asyncio
import sys

import click

from dagster_codekit.__version__ import __version__
from dagster_codekit.config import load_config
from dagster_codekit.utils.logging import configure_logging


@click.group()
@click.version_option(version=__version__, prog_name="dagster-codekit")
def cli():
    """
    dagster-codekit - Bridge between CI/CD and Dagster OSS.

    Auto-reload Dagster code locations when deployments complete.
    """
    pass


@cli.command()
@click.option(
    "--config",
    "-c",
    default="config.yaml",
    type=click.Path(exists=True),
    help="Path to config.yaml file",
)
@click.option(
    "--host",
    default=None,
    help="Override server host from config",
)
@click.option(
    "--port",
    default=None,
    type=int,
    help="Override server port from config",
)
@click.option(
    "--log-level",
    default="INFO",
    type=click.Choice(["DEBUG", "INFO", "WARNING", "ERROR"], case_sensitive=False),
    help="Set logging level",
)
def start(config: str, host: str | None, port: int | None, log_level: str):
    """
    Start dagster-codekit server.

    Loads configuration, validates it, and starts HTTP webhook server.

    Example:
        dagster-codekit start
        dagster-codekit start --config /etc/dagster-codekit/config.yaml
        dagster-codekit start --port 9000 --log-level DEBUG
    """
    # 1. Configure Logging First
    configure_logging(log_level)

    click.echo(f"🚀 Starting dagster-codekit v{__version__}")

    # 2. Load and validate config
    click.echo(f"📋 Loading config from: {config}")
    cfg = load_config(config)

    # 3. Apply CLI overrides
    if host:
        cfg.server.host = host
    if port:
        cfg.server.port = port

    click.echo(f"✅ Config validated successfully")
    click.echo(f"🌐 Server starting on {cfg.server.host}:{cfg.server.port}")

    # 4. Show enabled backends (Adaptado para o novo modelo tipado)
    enabled_backends = []
    if cfg.argocd:
        enabled_backends.append("argocd")
    if cfg.github:
        enabled_backends.append("github")

    if not enabled_backends:
        click.echo("⚠️  WARNING: No backends enabled! Webhooks will return 404.")
    else:
        click.echo(f"🔌 Enabled backends: {', '.join(enabled_backends)}")

    # 5. Run server
    from dagster_codekit.api.app import run_server

    try:
        asyncio.run(run_server(cfg))
    except KeyboardInterrupt:
        click.echo("\n👋 Shutting down gracefully...")
        sys.exit(0)


@cli.command()
def init():
    """
    Generate example config.yaml.
    """
    example_config = """# dagster-codekit configuration
# See: https://github.com/maltzsama/dagster-codekit

# HTTP Server
server:
  host: 0.0.0.0
  port: 8000

# Dagster Connection
dagster:
  webserver_url: http://dagster-webserver:3000
  
  # Optional: Authentication (if Dagster has auth enabled)
  # auth:
  #   type: header
  #   header_name: X-Auth-Token
  #   token_env: DAGSTER_AUTH_TOKEN

# Workspace Management
workspace:
  mode: configmap  # 'file' or 'configmap'
  
  # If mode=file (Docker Compose, VMs)
  # file:
  #   path: /opt/dagster/workspace.yaml
  
  # If mode=configmap (Kubernetes)
  configmap:
    namespace: dagster
    name: dagster-workspace
    max_retries: 5

# CI/CD Backends
backends:
  # ArgoCD (Recommended for Kubernetes/Helm/Kustomize)
  argocd:
    enabled: true
    webhook_secret: CHANGE_ME  # Generate with: openssl rand -hex 32
    grpc_timeout: 60
  
  # GitHub Actions (Alternative for direct push)
  # github:
  #   enabled: false
  #   webhook_secret: CHANGE_ME
  #   repositories:
  #     - repo: company/data-platform
  #       locations:
  #         - name: analytics
  #           grpc_host: analytics.dagster.svc.cluster.local
  #           grpc_port: 4000
"""
    click.echo(example_config)


@cli.command()
@click.argument("config_path", default="config.yaml")
def validate(config_path: str):
    """
    Validate configuration file.

    Checks that config.yaml is valid and all required fields are present.
    """
    click.echo(f"🔍 Validating config: {config_path}")

    # load_config will exit(1) with clear errors if invalid
    cfg = load_config(config_path)

    click.echo(f"✅ Config is valid!")
    click.echo(f"\n📊 Configuration summary:")
    click.echo(f"  Server: {cfg.server.host}:{cfg.server.port}")
    click.echo(f"  Dagster: {cfg.dagster.webserver_url}")
    click.echo(f"  Workspace mode: {cfg.workspace.mode}")

    # Show enabled backends
    enabled_backends = []
    if cfg.argocd:
        enabled_backends.append("argocd")
    if cfg.github:
        enabled_backends.append("github")

    if enabled_backends:
        click.echo(f"\n🔌 Enabled backends:")
        for name in enabled_backends:
            click.echo(f"  • {name}")
    else:
        click.echo(f"\n⚠️  No backends enabled!")


@cli.command()
def version():
    """
    Show version information.
    """
    click.echo(f"dagster-codekit version {__version__}")
    click.echo(f"Python {sys.version}")


def main():
    """Entry point for the CLI."""
    cli()


if __name__ == "__main__":
    main()
