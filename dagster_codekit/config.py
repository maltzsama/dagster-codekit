import os
import sys
from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, Field, field_validator


class ServerConfig(BaseModel):
    host: str = "0.0.0.0"
    port: int = Field(8000, description="HTTP API port (FastAPI) for CI/CD pushes")
    grpc_port: int = Field(4000, description="gRPC proxy port for Dagster webserver")
    workers: int = Field(4, ge=1, description="ThreadPool workers for gRPC server")


class DatabaseConfig(BaseModel):
    url: str = Field("sqlite:///./codekit.db", description="Peewee connection URL")
    pool_size: int = 5
    max_overflow: int = 10


class K8sConfig(BaseModel):
    namespace: str = Field("dagster", description="Kubernetes namespace to spawn jobs into")
    service_account: str = Field(
        "dagster", description="Service account with permissions to access storage/DB"
    )
    image_pull_policy: Literal["Always", "IfNotPresent", "Never"] = "Always"
    ttl_seconds_after_finished: int = Field(
        300, description="Auto-cleanup jobs after N seconds"
    )
    forward_env_vars: list[str] = Field(
        default=[
            "DAGSTER_POSTGRES_USER",
            "DAGSTER_POSTGRES_PASSWORD",
            "DAGSTER_POSTGRES_DB",
            "DAGSTER_POSTGRES_HOSTNAME",
            "AWS_ACCESS_KEY_ID",
            "AWS_SECRET_ACCESS_KEY",
            "CEPH_ENDPOINT_URL",
            "CEPH_BUCKET_NAME",
        ],
        description="Environment variables forwarded from proxy to worker jobs",
    )


class DockerConfig(BaseModel):
    network: str = Field("host", description="Docker network mode")
    auto_remove: bool = Field(True, description="Remove container after exit")
    forward_env_vars: list[str] = Field(
        default=[
            "DAGSTER_POSTGRES_USER",
            "DAGSTER_POSTGRES_PASSWORD",
            "DAGSTER_POSTGRES_DB",
            "DAGSTER_POSTGRES_HOSTNAME",
            "AWS_ACCESS_KEY_ID",
            "AWS_SECRET_ACCESS_KEY",
        ],
        description="Environment variables forwarded from proxy to worker container",
    )


class LauncherConfig(BaseModel):
    enabled: bool = True
    mode: Literal["k8s", "docker"] = Field("k8s", description="Use 'docker' for local dev")
    k8s: K8sConfig = Field(default_factory=K8sConfig)
    docker: DockerConfig = Field(default_factory=DockerConfig)


class ArgoCDBackendConfig(BaseModel):
    enabled: bool = False
    webhook_secret: str = Field("", description="ArgoCD webhook HMAC secret")
    grpc_timeout: int = Field(30, description="Seconds to wait for gRPC health check")
    grpc_tls: bool = Field(False, description="Use TLS for gRPC connection")
    check_interval: float = Field(1.0, description="Interval between gRPC health check retries")


class BackendsConfig(BaseModel):
    argocd: ArgoCDBackendConfig = Field(default_factory=ArgoCDBackendConfig)


class AuthConfig(BaseModel):
    enabled: bool = False
    tokens: list[str] = Field(default_factory=list, description="List of valid Bearer tokens")

    @field_validator("tokens")
    @classmethod
    def validate_tokens(cls, v, info):
        if info.data.get("enabled") and not v:
            raise ValueError("Auth is enabled but no tokens were provided.")
        return v


class LoggingConfig(BaseModel):
    level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "INFO"
    format: Literal["console", "json"] = "console"


class Config(BaseModel):
    server: ServerConfig = Field(default_factory=ServerConfig)
    database: DatabaseConfig = Field(default_factory=DatabaseConfig)
    launcher: LauncherConfig = Field(default_factory=LauncherConfig)
    backends: BackendsConfig = Field(default_factory=BackendsConfig)
    auth: AuthConfig = Field(default_factory=AuthConfig)
    logging: LoggingConfig = Field(default_factory=LoggingConfig)


def load_config(path: str | Path = "config.yaml") -> Config:
    path_obj = Path(path)

    if not path_obj.exists():
        return Config()

    try:
        with open(path_obj) as f:
            raw_content = f.read()
            expanded_content = os.path.expandvars(raw_content)
            data = yaml.safe_load(expanded_content) or {}

        return Config(**data)

    except Exception as e:
        sys.stderr.write(f"Configuration Load Error ({path}):\n{str(e)}\n")
        sys.exit(1)
