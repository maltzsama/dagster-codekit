import sys
import asyncio
import click
import uvicorn
import httpx
import structlog
from pathlib import Path
from dataclasses import asdict

from dagster_codekit.__version__ import __version__
from dagster_codekit.config import load_config
from dagster_codekit.utils.logging import configure_logging

from dagster_codekit.core.engine import create_snapshot_payload

logger = structlog.get_logger(__name__)


@click.group()
@click.version_option(version=__version__, prog_name="dagster-codekit")
def cli():
    """
    dagster-codekit - Metadata Control Plane for Dagster OSS.
    """
    pass


# COMMAND: start (Server)


@cli.command()
@click.option(
    "--config",
    "-c",
    default="config.yaml",
    type=click.Path(exists=True),
    help="Path to config.yaml file",
)
@click.option("--port", default=None, type=int, help="Override API port")
@click.option("--log-level", default="INFO", help="Set logging level")
def start(config: str, port: int | None, log_level: str):
    """
    Start the Codekit API & gRPC Proxy.
    """
    configure_logging(log_level)
    click.echo(click.style(f"🚀 Starting dagster-codekit v{__version__}", fg="cyan", bold=True))

    cfg = load_config(config)
    if port:
        cfg.server.port = port

    click.echo(f"📋 Database: {cfg.database.url}")
    click.echo(f"🌐 API Server: http://{cfg.server.host}:{cfg.server.port}")

    uvicorn.run(
        "dagster_codekit.api.app:app",
        host=cfg.server.host,
        port=cfg.server.port,
        log_level=log_level.lower(),
        factory=False,
    )


# COMMAND: snapshot (The "Push")


@cli.command()
@click.option("--location", "-l", required=True, help="Name of the code location")
@click.option(
    "--file", "-f", required=True, type=click.Path(exists=True), help="Path to definitions.py"
)
@click.option("--image", "-i", required=True, help="Docker image tag (e.g. repo:tag)")
@click.option("--url", default="http://localhost:8000", help="Codekit API URL")
@click.option("--token", envvar="CODEKIT_TOKEN", help="Authentication token")
def snapshot(location: str, file: str, image: str, url: str, token: str | None):
    """
    Generate and push a metadata snapshot to the Codekit server.
    """
    configure_logging("INFO")

    click.echo(f"📸 Generating snapshot for location: {click.style(location, fg='green')}")
    click.echo(f"   File: {file}")
    click.echo(f"   Image: {image}")

    try:
        payload = create_snapshot_payload(location, file, image)
        json_size = len(payload.snapshot_json) / 1024
        click.echo(f"📦 Snapshot generated: {json_size:.2f} KB")

        headers = {"Authorization": f"Bearer {token}"} if token else {}

        click.echo(f"📤 Pushing to {url}/deploy...")
        click.echo(f"Payload: {asdict(payload)}")

        response = httpx.post(
            f"{url}/deploy", json=asdict(payload), headers=headers, timeout=30.0
        )

        if response.status_code == 200:
            click.echo(click.style("✅ Deploy successful!", fg="green", bold=True))
            click.echo(f"   Response: {response.json()}")
        else:
            click.echo(click.style(f"❌ Deploy failed: {response.status_code}", fg="red"))
            click.echo(f"   Error: {response.text}")
            sys.exit(1)

    except Exception as e:
        click.echo(click.style(f"❌ Fatal error: {str(e)}", fg="red"), err=True)
        sys.exit(1)


# COMMAND: init
@cli.command()
def init():
    """Generate a snapshot-ready config.yaml."""
    example = """# dagster-codekit configuration
server:
  host: 0.0.0.0
  port: 8000
  grpc_port: 4000
  workers: 4

database:
  url: "sqlite:///./codekit.db"
  pool_size: 5
  max_overflow: 10

launcher:
  enabled: true
  mode: k8s
  k8s:
    namespace: dagster
    service_account: dagster
    image_pull_policy: Always
    ttl_seconds_after_finished: 300
    forward_env_vars:
      - DAGSTER_POSTGRES_USER
      - DAGSTER_POSTGRES_PASSWORD
      - DAGSTER_POSTGRES_DB
      - DAGSTER_POSTGRES_HOSTNAME
      - AWS_ACCESS_KEY_ID
      - AWS_SECRET_ACCESS_KEY
  docker:
    network: host
    auto_remove: true
    forward_env_vars:
      - DAGSTER_POSTGRES_USER
      - DAGSTER_POSTGRES_PASSWORD
      - DAGSTER_POSTGRES_DB
      - DAGSTER_POSTGRES_HOSTNAME

backends:
  argocd:
    enabled: false
    webhook_secret: "generate-a-long-secret-here"
    grpc_timeout: 30
    grpc_tls: false
    check_interval: 1.0

auth:
  enabled: false
  tokens: []

logging:
  level: INFO
  format: console
"""
    with open("config.yaml", "w") as f:
        f.write(example)
    click.echo("Created config.yaml with default settings.")


def main():
    cli()


if __name__ == "__main__":
    main()
