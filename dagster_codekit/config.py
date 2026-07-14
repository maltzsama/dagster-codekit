import os
import sys
import yaml
from pathlib import Path
from typing import List, Literal, Optional

from pydantic import BaseModel, Field, field_validator


# =========================================================
# 1. SERVER (The Interface)
# =========================================================
class ServerConfig(BaseModel):
    """
    Configures the two entrypoints of the application.
    """

    host: str = "0.0.0.0"
    port: int = Field(8000, description="HTTP API Port (FastAPI) for CI/CD Pushes")
    grpc_port: int = Field(4000, description="gRPC Proxy Port for Dagster Webserver")
    workers: int = Field(4, ge=1, description="ThreadPool workers for gRPC server")


# =========================================================
# 2. DATABASE (The Memory)
# =========================================================
class DatabaseConfig(BaseModel):
    """
    Persistence for Repository Snapshots.
    """

    url: str = Field("sqlite:///./codekit.db", description="SQLAlchemy Connection String")
    pool_size: int = 5
    max_overflow: int = 10


# =========================================================
# 3. LAUNCHER (The Muscle)
# =========================================================
class K8sConfig(BaseModel):
    """
    Kubernetes-specific execution settings.
    """

    namespace: str = Field("dagster", description="K8s Namespace to spawn jobs into")
    service_account: str = Field("dagster", description="K8s SA with permissions to access S3/DB")
    image_pull_policy: Literal["Always", "IfNotPresent", "Never"] = "Always"
    ttl_seconds_after_finished: int = Field(300, description="Auto-cleanup jobs after X seconds")

    # Environment variables to copy from Proxy -> Worker Job
    # This allows the Worker to inherit DB credentials without hardcoding them in the image
    forward_env_vars: List[str] = Field(
        default=[
            "DAGSTER_POSTGRES_USER",
            "DAGSTER_POSTGRES_PASSWORD",
            "DAGSTER_POSTGRES_DB",
            "DAGSTER_POSTGRES_HOSTNAME",
            "AWS_ACCESS_KEY_ID",
            "AWS_SECRET_ACCESS_KEY",
            "CEPH_ENDPOINT_URL",
            "CEPH_BUCKET_NAME",
        ]
    )


class LauncherConfig(BaseModel):
    """
    Determines how execution runs are spawned.
    """

    enabled: bool = True
    mode: Literal["k8s", "docker"] = Field("k8s", description="Use 'docker' only for local dev")
    k8s: K8sConfig = Field(default_factory=K8sConfig)


# =========================================================
# 4. AUTHENTICATION (The Bouncer)
# =========================================================
class AuthConfig(BaseModel):
    """
    Protects the HTTP /deploy endpoint.
    """

    enabled: bool = False
    tokens: List[str] = Field(default_factory=list, description="List of valid Bearer tokens")

    @field_validator("tokens")
    @classmethod
    def validate_tokens(cls, v, info):
        if info.data.get("enabled") and not v:
            raise ValueError("Auth is enabled but no tokens were provided.")
        return v


# =========================================================
# 5. LOGGING
# =========================================================
class LoggingConfig(BaseModel):
    level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "INFO"
    format: Literal["console", "json"] = "console"


# =========================================================
# ROOT CONFIGURATION
# =========================================================
class Config(BaseModel):
    server: ServerConfig = Field(default_factory=ServerConfig)
    database: DatabaseConfig = Field(default_factory=DatabaseConfig)
    launcher: LauncherConfig = Field(default_factory=LauncherConfig)
    auth: AuthConfig = Field(default_factory=AuthConfig)
    logging: LoggingConfig = Field(default_factory=LoggingConfig)


# =========================================================
# LOADER LOGIC
# =========================================================
def load_config(path: str | Path = "config.yaml") -> Config:
    """
    Loads YAML config with ENV VAR expansion support (e.g. ${DB_PASSWORD}).
    Returns default config if file is missing.
    """
    path_obj = Path(path)

    if not path_obj.exists():
        # Return defaults for quick local start
        return Config()

    try:
        with open(path_obj, "r") as f:
            raw_content = f.read()
            # Expand ${VAR} environment variables in YAML before parsing
            expanded_content = os.path.expandvars(raw_content)
            data = yaml.safe_load(expanded_content) or {}

        return Config(**data)

    except Exception as e:
        # Fail hard if config is invalid
        sys.stderr.write(f"❌ Configuration Load Error ({path}):\n{str(e)}\n")
        sys.exit(1)
