import os
import sys
from pathlib import Path
from typing import Literal, Optional, Any

import yaml
from pydantic import BaseModel, Field, field_validator, model_validator, ValidationInfo


class LocationConfig(BaseModel):
    name: str
    grpc_host: str
    grpc_port: int = Field(4000, ge=1, le=65535)
    path: Optional[str] = None

    @field_validator("name")
    @classmethod
    def validate_name(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("location name cannot be empty")
        if " " in v:
            raise ValueError(f"location name '{v}' contains spaces")
        return v.strip()

    @field_validator("grpc_host")
    @classmethod
    def validate_grpc_host(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("grpc_host cannot be empty")
        if v.startswith(("http://", "https://")):
            raise ValueError(f"grpc_host '{v}' should not include protocol")
        return v.strip()


class RepositoryConfig(BaseModel):
    repo: str
    locations: list[LocationConfig] = Field(..., min_length=1)

    @field_validator("repo")
    @classmethod
    def validate_repo(cls, v: str) -> str:
        if "/" not in v:
            raise ValueError(f"Invalid repo format '{v}'. Expected 'owner/repo'")
        return v

    @model_validator(mode="after")
    def validate_unique_locations(self):
        names = [loc.name for loc in self.locations]
        if len(names) != len(set(names)):
            raise ValueError(f"Duplicate location names in repo '{self.repo}'")
        return self


# --- BACKENDS ---


class ArgoCDBackendConfig(BaseModel):
    enabled: bool = False
    webhook_secret: str
    grpc_timeout: int = Field(60, ge=10, le=300)

    @field_validator("webhook_secret")
    @classmethod
    def validate_secret(cls, v: str, info: ValidationInfo) -> str:
        if info.data.get("enabled") and len(v) < 20:
            raise ValueError("webhook_secret is too short (minimum 20 characters)")
        return v


class GitHubBackendConfig(BaseModel):
    enabled: bool = False
    webhook_secret: str
    repositories: list[RepositoryConfig] = Field(default_factory=list)

    @field_validator("webhook_secret")
    @classmethod
    def validate_secret(cls, v: str, info: ValidationInfo) -> str:
        if info.data.get("enabled") and len(v) < 20:
            raise ValueError("webhook_secret is too short")
        return v

    @model_validator(mode="after")
    def validate_repos(self):
        if self.enabled and not self.repositories:
            raise ValueError("GitHub backend is enabled but no repositories configured")
        return self


# --- AUTH & DAGSTER ---
class AuthConfig(BaseModel):
    type: Literal["none", "header", "bearer", "basic"] = "none"
    header_name: Optional[str] = None
    token_env: Optional[str] = None
    token: Optional[str] = None
    username: Optional[str] = None
    password_env: Optional[str] = None
    password: Optional[str] = None

    @model_validator(mode="after")
    def validate_auth_logic(self):
        if self.type == "header":
            if not self.header_name:
                raise ValueError("requires 'header_name'")
            if not self.token and not self.token_env:
                raise ValueError("requires either 'token_env' or 'token'")
        if self.type == "basic" and not self.username:
            raise ValueError("requires 'username'")
        return self


class DagsterConfig(BaseModel):
    webserver_url: str
    auth: AuthConfig = Field(default_factory=AuthConfig)

    @field_validator("webserver_url")
    @classmethod
    def validate_url(cls, v: str) -> str:
        if not v.startswith(("http://", "https://")):
            raise ValueError("Must start with http:// or https://")
        return v.rstrip("/")


# --- WORKSPACE & SERVER ---
class WorkspaceConfig(BaseModel):
    mode: Literal["file", "configmap"]
    file: dict[str, Any] = Field(default_factory=dict)
    configmap: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_mode_config(self):
        if self.mode == "file" and not self.file:
            raise ValueError("workspace.file not configured")
        if self.mode == "configmap" and not self.configmap:
            raise ValueError("workspace.configmap not configured")
        return self


class ServerConfig(BaseModel):
    host: str = "0.0.0.0"
    port: int = Field(8000, ge=1, le=65535)


# --- ROOT CONFIG ---
class BackendsConfig(BaseModel):
    argocd: Optional[ArgoCDBackendConfig] = None
    github: Optional[GitHubBackendConfig] = None

    @model_validator(mode="after")
    def validate_at_least_one_enabled(self):
        enabled = any([self.argocd and self.argocd.enabled, self.github and self.github.enabled])
        if not enabled:
            raise ValueError("No backends enabled")
        return self


class Config(BaseModel):
    server: ServerConfig = Field(default_factory=ServerConfig)  # <--- CORREÇÃO AQUI
    dagster: DagsterConfig
    workspace: WorkspaceConfig
    backends: BackendsConfig

    @property
    def argocd(self):
        return (
            self.backends.argocd if self.backends.argocd and self.backends.argocd.enabled else None
        )

    @property
    def github(self):
        return (
            self.backends.github if self.backends.github and self.backends.github.enabled else None
        )


# --- LOADER ---
def load_config(path: str | Path) -> Config:
    path = Path(path)
    if not path.exists():
        sys.stderr.write(f"Config file not found: {path}\n")
        sys.exit(1)

    try:
        with open(path) as f:
            raw_data = f.read()
            data = yaml.safe_load(os.path.expandvars(raw_data)) or {}
        return Config(**data)
    except yaml.YAMLError as e:
        sys.stderr.write(f"Invalid YAML: {e}\n")
        sys.exit(1)
    except Exception as e:
        sys.stderr.write(f"Configuration Error:\n{e}\n")
        sys.exit(1)
