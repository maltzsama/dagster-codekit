# 📡 Dagster CodeKit

[![Python Version](https://img.shields.io/badge/python-3.10%2B-blue.svg?logo=python&logoColor=white)](https://www.python.org/downloads/)
[![Build Status](https://github.com/maltzsama/dagster-codekit/workflows/CI/badge.svg)](https://github.com/maltzsama/dagster-codekit/actions)
[![License](https://img.shields.io/badge/license-Apache%202.0-green.svg?logo=open-source-initiative&logoColor=white)](https://opensource.org/licenses/Apache-2.0)
[![Coverage](https://codecov.io/gh/maltzsama/dagster-codekit/branch/main/graph/badge.svg)](https://codecov.io/gh/maltzsama/dagster-codekit)
[![PyPI Version](https://img.shields.io/pypi/v/dagster-codekit?color=blue&logo=pypi&logoColor=white)](https://pypi.org/project/dagster-codekit/)
[![Downloads](https://img.shields.io/pypi/dm/dagster-codekit?logo=pypi&logoColor=white)](https://pypi.org/project/dagster-codekit/)

**Serverless Metadata Control Plane for Dagster OSS.**

*Decouple CI/CD from execution. No always-running code servers. Ephemeral workers on demand.*

---

## 🎯 What is this?

Dagster code servers run 24/7, loading heavy user code just to serve metadata to the webserver. Every deploy requires restarting them. In Kubernetes, you pay for idle pods.

**CodeKit solves this by separating metadata from execution:**

* 📸 **Snapshot at build time:** Extract repository metadata during CI/CD.
* 📡 **Serve via gRPC proxy:** The proxy serves lightweight static JSON without loading user code.
* 🚀 **Spawn on demand:** Execution happens in ephemeral Kubernetes Jobs (or Docker containers) that spin up, run, and die.

**No idle code servers. Decoupled deploys. Serverless execution.**

---

## 🏗️ Architecture

```mermaid
flowchart TB
    subgraph CI/CD["CI/CD Pipeline"]
        CLI["dagster-codekit snapshot"]
    end

    subgraph Codekit["Codekit Server"]
        API["FastAPI :8000"]
        gRPC["gRPC Proxy :4000"]
        DB[("Database<br/>Snapshots + Locations")]
    end

    subgraph K8s["Kubernetes"]
        Job1["Run Worker Job"]
        Job2["Run Worker Job"]
    end

    subgraph Dagster["Dagster"]
        Webserver["dagster-webserver"]
    end

    CLI -->|"POST /deploy"| API
    API --> DB
    Webserver -->|"gRPC metadata"| gRPC
    gRPC -->|"query"| DB
    gRPC -->|"spawn"| Job1
    gRPC -->|"spawn"| Job2
    Job1 -->|"report status"| Webserver
    Job2 -->|"report status"| Webserver
```

---

## ✨ Features

* ✅ **Zero idle code servers** — Metadata is served as static JSON via gRPC.
* ✅ **26/26 gRPC methods** — Full `DagsterApiServicer` implementation.
* ✅ **Kubernetes launcher** — Ephemeral Jobs with configurable resources, per-location overrides.
* ✅ **Docker launcher** — Local dev mode without a cluster.
* ✅ **Per-location K8s config** — Resources, service accounts, node selectors, image pull secrets.
* ✅ **CLI snapshot + deploy** — Extract and push metadata from CI/CD.
* ✅ **Snapshot history** — Rollback to any previous snapshot.
* ✅ **Thread-safe database** — Pooled connections for concurrent gRPC requests.
* ✅ **Health checks** — `/health/ready`, `/health/live` with DB and gRPC verification.
* ✅ **Prometheus metrics** — Deployments, runs, gRPC requests, locations count.
* ✅ **Token auth** — Protect the `/deploy` endpoint.
* ✅ **Multi-backend webhooks** — ArgoCD, Forgejo, Azure DevOps native integration.
* ✅ **Rate limiting** — In-memory and Redis-backed abuse protection on ingestion endpoints.
* ✅ **Docker Compose** — One-command local deployment (SQLite and Postgres profiles).
* ✅ **Real cancellation** — `CancelExecution` deletes K8s Jobs / Docker containers.
* ✅ **Sensor & schedule support** — Ephemeral tick evaluation workers.

---

## 🚀 Quick Start

### Fastest path — Docker Compose

```bash
cd examples/docker-compose
docker compose --profile sqlite up
```

CodeKit is now running on `http://localhost:8000` (API) and `localhost:4000` (gRPC).
Skip to step 4 to deploy your first code location.

### Manual install

```bash
# 1. Install
pip install dagster-codekit

# With Kubernetes support
pip install dagster-codekit[kubernetes]

# With Postgres support
pip install dagster-codekit[postgres]

# 2. Initialize config
dagster-codekit init

# 3. Start the server
dagster-codekit start
```

Starts the API on `:8000` and the gRPC proxy on `:4000`.

### Deploy a code location from CI/CD

```bash
dagster-codekit snapshot \
  --location my-pipeline \
  --file definitions.py \
  --image registry.example.com/my-pipeline:v1
```

This generates a metadata snapshot and pushes it to the Codekit API.

### 5. Configure Dagster webserver

Point your `dagster-workspace.yaml` at the gRPC proxy:

```yaml
load_from:
  - grpc_server:
      host: localhost
      port: 4000
      location_name: my-pipeline
```

### 6. Dry-run (validate without pushing)

```bash
dagster-codekit snapshot \
  --location my-pipeline \
  --file definitions.py \
  --image registry.example.com/my-pipeline:v1 \
  --dry-run
```

Output:
```
Snapshot generated: 142.35 KB
   Jobs: 12
   Assets: 47
   Schedules: 3
   Sensors: 2
Dry-run complete, nothing pushed.
```

---

## 🔄 CI/CD Flow

```mermaid
sequenceDiagram
    participant CI as CI/CD
    participant API as Codekit API
    participant DB as Database
    participant gRPC as gRPC Proxy
    participant WS as Dagster Webserver
    participant K8s as Kubernetes

    CI->>CI: pip install dagster-codekit
    CI->>CI: dagster-codekit snapshot
    CI->>API: POST /deploy (snapshot JSON)
    API->>DB: store location + snapshot

    WS->>gRPC: ListRepositories
    gRPC->>DB: query locations
    gRPC->>WS: [my-pipeline, ...]

    WS->>gRPC: ExternalRepository (my-pipeline)
    gRPC->>DB: read snapshot
    gRPC->>WS: job graph, assets, schedules

    WS->>gRPC: StartRun (my-pipeline, run-id)
    gRPC->>DB: lookup image tag
    gRPC->>K8s: create Job (image + args)
    K8s->>WS: report run status
```

---

## 📂 CLI Commands

| Command | Description |
|---------|-------------|
| `dagster-codekit init` | Generate a default `config.yaml` |
| `dagster-codekit start` | Start the API and gRPC proxy |
| `dagster-codekit snapshot` | Extract and push a metadata snapshot |
| `dagster-codekit delete` | Delete a code location and its snapshots |
| `dagster-codekit rollback` | Revert a location to the previous snapshot |

---

## ⚙️ Configuration

```yaml
server:
  host: 0.0.0.0
  port: 8000          # HTTP API
  grpc_port: 4000     # gRPC proxy
  workers: 4

database:
  url: "sqlite:///./codekit.db"
  # url: "postgresql://user:pass@host:5432/codekit"

launcher:
  enabled: true
  mode: k8s            # or 'docker' for local dev
  k8s:
    namespace: dagster
    service_account: dagster
    image_pull_policy: Always
    ttl_seconds_after_finished: 300
    forward_env_vars:
      - DAGSTER_POSTGRES_USER
      - DAGSTER_POSTGRES_PASSWORD
  docker:
    network: host
    auto_remove: true

backends:
  argocd:
    enabled: false
    webhook_secret: ""
    grpc_timeout: 30
  forgejo:
    enabled: false
    webhook_secret: ""
    payload_mode: custom
  azure_devops:
    enabled: false
    webhook_secret: ""
    header_name: X-Codekit-Webhook-Token

auth:
  enabled: false
  tokens: []

rate_limit:
  enabled: false
  requests_per_minute: 60
  backend: memory       # or 'redis' for multi-pod
  redis_url: null

logging:
  level: INFO
  format: console       # or 'json' for structured logging
```

---

## 🔗 CI/CD Backend Webhooks

Instead of calling `/deploy` with CodeKit's internal schema, configure your CI/CD
tool to send webhooks directly to CodeKit:

| Backend | Webhook URL | Auth Method |
|---------|------------|-------------|
| **ArgoCD** | `POST /webhooks/argocd` | HMAC secret (`X-Argocd-Webhook-Secret`) |
| **Forgejo** | `POST /webhooks/forgejo` | HMAC-SHA256 (`X-Forgejo-Webhook-Secret`) |
| **Azure DevOps** | `POST /webhooks/azure_devops` | Custom header token |

Each backend parses its native payload format and maps it to a `DeploymentEvent`
automatically. For tools without a dedicated backend, use `POST /deploy` directly
with a Bearer token.

**Required Kubernetes labels** (ArgoCD):
```yaml
metadata:
  labels:
    dagster.io/code-location: "true"
    dagster.io/location-name: "my-pipeline"
    dagster.io/image-tag: "registry.example.com/my-pipeline:v1"
```

**Required Azure DevOps pipeline variables**:
```
dagster.location_name = my-pipeline
dagster.image_tag = registry.example.com/my-pipeline:v1
```

---

## 🐳 Docker Launcher (Local Dev)

Set `launcher.mode: docker` in `config.yaml`. The proxy spawns `docker run` containers instead of K8s jobs.

```yaml
launcher:
  mode: docker
  docker:
    network: host
    auto_remove: true
    forward_env_vars:
      - DAGSTER_POSTGRES_USER
      - DAGSTER_POSTGRES_PASSWORD
```

---

## ☸️ Per-Location K8s Configuration

Override global defaults per code location. Pass `k8s_config` in the deploy payload:

```json
{
  "location_name": "gpu-pipeline",
  "image_tag": "registry.example.com/gpu:v1",
  "snapshot_json": "...",
  "k8s_config": {
    "service_account": "gpu-sa",
    "resources": {
      "requests": {"cpu": "2", "memory": "8Gi", "nvidia.com/gpu": "1"},
      "limits": {"cpu": "4", "memory": "16Gi", "nvidia.com/gpu": "1"}
    },
    "node_selector": {"node-type": "gpu"},
    "image_pull_secrets": [{"name": "registry-credentials"}],
    "labels": {"team": "ml"},
    "env": {"EXTRA_VAR": "value"}
  }
}
```

Supported overrides: `service_account`, `namespace`, `image_pull_policy`, `ttl_seconds_after_finished`, `resources`, `labels`, `annotations`, `env`, `image_pull_secrets`, `node_selector`.

---

## 📊 API Endpoints

| Method | Path | Auth | Description |
|--------|------|------|-------------|
| `GET` | `/health` | No | Full health: DB + gRPC |
| `GET` | `/health/ready` | No | Readiness probe (DB check) |
| `GET` | `/health/live` | No | Liveness probe |
| `GET` | `/metrics` | No | Prometheus metrics |
| `POST` | `/deploy` | Token | Push a metadata snapshot |
| `POST` | `/webhooks/{backend}` | Per-backend | CI/CD webhook (argocd, forgejo, azure_devops) |
| `DELETE` | `/locations/{name}` | Token | Delete location + snapshots |
| `POST` | `/locations/{name}/rollback` | Token | Rollback to previous snapshot |

---

## 🔮 Roadmap

### Current (v0.1.0)

* ✅ Full gRPC proxy (26/26 methods)
* ✅ Kubernetes launcher with per-location config
* ✅ Docker launcher for local dev
* ✅ Snapshot extraction and CI/CD push
* ✅ Rollback to previous snapshots
* ✅ Thread-safe database pooling
* ✅ Health checks and Prometheus metrics
* ✅ ArgoCD, Forgejo, Azure DevOps webhook backends
* ✅ Rate limiting (memory + Redis)
* ✅ Real run cancellation
* ✅ Sensor & schedule support via ephemeral workers
* ✅ Docker Compose + K8s example manifests

### Next

* 🔄 Helm chart
* 🔄 Multi-repository locations
* 🔄 gRPC TLS support
* 🔄 OIDC/OAuth backend support

---

## 🛡️ Integration with Dagster AuthKit

CodeKit handles the **code server layer** (gRPC + execution). For webserver authentication, pair it with [Dagster AuthKit](https://github.com/maltzsama/dagster-authkit):

```mermaid
flowchart LR
    Browser["Browser"] --> AuthKit["AuthKit + Webserver<br/>:3000"]
    AuthKit -->|"gRPC"| Codekit["Codekit Proxy<br/>:4000"]
    Codekit -->|"spawn"| K8s["K8s Jobs"]
    Codekit -->|"store"| DB[("DB")]
```

AuthKit provides login, RBAC, and audit logs at the webserver layer. CodeKit provides serverless metadata and execution. They run side-by-side with zero code changes.

---

## 📄 License

Apache 2.0 — see [LICENSE](LICENSE)

---

## 🙏 Credits

Built by [Demetrius Albuquerque](https://github.com/maltzsama/) because self-hosting Dagster shouldn't mean paying for idle code servers.
