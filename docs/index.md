# Dagster CodeKit

Serverless Metadata Control Plane for Dagster OSS.

Decouples code definitions (CI/CD) from execution (Kubernetes), enabling
zero-infrastructure deployments via a gRPC proxy architecture.

## Components

- **CLI**: Extracts metadata snapshots during CI/CD.
- **Proxy**: Serves metadata to Dagster Webserver without loading user code.
- **Launcher**: Spawns ephemeral Kubernetes jobs (or Docker containers) for execution.

## Quick Start

```bash
# Install
pip install dagster-codekit

# Initialize config
dagster-codekit init

# Start the server
dagster-codekit start

# Deploy a code location from CI/CD
dagster-codekit snapshot --location my-pipeline --file definitions.py --image registry.example.com/my-pipeline:v1
```

## Architecture

```
CI/CD Pipeline                Codekit Server               Kubernetes
─────────────                ───────────────              ───────────
dagster-codekit snapshot     FastAPI + gRPC Proxy         Ephemeral Jobs
        │                           │                          │
        ├─ extract metadata ───────►├─ store snapshot          │
        ├─ push JSON ──────────────►├─ serve gRPC ◄── dagit ──┤
        │                           ├─ launch job ────────────►
        │                           │                    ┌─────┴─────┐
        │                           │                    │ dagster   │
        │                           │◄── report status ──│ api        │
        │                           │                    │ execute_run│
        │                           │                    └───────────┘
```
