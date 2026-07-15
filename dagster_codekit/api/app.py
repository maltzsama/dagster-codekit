from contextlib import asynccontextmanager

import structlog
from fastapi import BackgroundTasks, Depends, FastAPI, Header, HTTPException, Request
from starlette.responses import Response

from dagster_codekit.__version__ import __version__
from dagster_codekit.api.middleware import MemoryRateLimiter, RateLimitMiddleware, RedisRateLimiter
from dagster_codekit.api.schemas import DeploymentEvent
from dagster_codekit.backends.registry import BackendRegistry
from dagster_codekit.config import load_config
from dagster_codekit.core.exceptions import AuthenticationError
from dagster_codekit.core.grpc_proxy import run_grpc_server
from dagster_codekit.db.models import CodeLocation, Snapshot, db_session, init_db
from dagster_codekit.utils.metrics import (
    deployments_total,
    get_metrics_response,
    locations_count,
    snapshots_count,
)

logger = structlog.get_logger(__name__)

cfg = load_config("config.yaml")

grpc_server_instance = None
backend_registry: BackendRegistry | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global grpc_server_instance, backend_registry
    logger.info("startup", msg="Initializing Codekit...")

    init_db(cfg.database.url)
    backend_registry = BackendRegistry(cfg.backends)
    logger.info("backends_registered", backends=backend_registry.names)

    grpc_server_instance = run_grpc_server(
        host="0.0.0.0",
        port=cfg.server.grpc_port,
        db_conn=None,
        max_workers=cfg.server.workers,
        launcher_mode=cfg.launcher.mode,
        k8s_namespace=cfg.launcher.k8s.namespace,
        k8s_service_account=cfg.launcher.k8s.service_account,
        k8s_image_pull_policy=cfg.launcher.k8s.image_pull_policy,
        k8s_ttl_seconds=cfg.launcher.k8s.ttl_seconds_after_finished,
        forward_env_vars=cfg.launcher.k8s.forward_env_vars,
        docker_network=cfg.launcher.docker.network,
        docker_auto_remove=cfg.launcher.docker.auto_remove,
        docker_forward_env_vars=cfg.launcher.docker.forward_env_vars,
    )

    yield

    logger.info("shutdown", msg="Stopping Codekit...")
    if grpc_server_instance:
        grpc_server_instance.stop(grace=5)


app = FastAPI(title="Dagster Codekit", version="1.0.0", lifespan=lifespan)

# Body size limit: 10 MB for ingestion endpoints
MAX_BODY_SIZE = 10 * 1024 * 1024

# Rate limiting middleware
if cfg.rate_limit.enabled:
    if cfg.rate_limit.backend == "redis":
        if not cfg.rate_limit.redis_url:
            logger.warning("rate_limit_redis_missing_url", msg="Redis backend selected but no redis_url configured")
        else:
            limiter = RedisRateLimiter(
                cfg.rate_limit.requests_per_minute,
                cfg.rate_limit.redis_url,
            )
            app.add_middleware(RateLimitMiddleware, limiter=limiter, key_by=cfg.rate_limit.key_by)
    else:
        limiter = MemoryRateLimiter(cfg.rate_limit.requests_per_minute)
        app.add_middleware(RateLimitMiddleware, limiter=limiter, key_by=cfg.rate_limit.key_by)


def verify_token(authorization: str = Header(None)):
    if not cfg.auth.enabled:
        return True

    if not authorization:
        raise HTTPException(status_code=401, detail="Missing Authorization Header")

    try:
        scheme, token = authorization.split()
        if scheme.lower() != "bearer":
            raise HTTPException(status_code=401, detail="Invalid Auth Scheme")
        if token not in cfg.auth.tokens:
            raise HTTPException(status_code=403, detail="Invalid Token")
    except ValueError:
        raise HTTPException(status_code=401, detail="Invalid Header Format") from None


def _register_deployment(event: DeploymentEvent):
    """Shared deployment registration used by both /deploy and webhook routes."""
    with db_session():
        location, _ = CodeLocation.get_or_create(
            name=event.location_name,
            defaults={
                "image": event.image_tag,
                "namespace": cfg.launcher.k8s.namespace,
            },
        )

        if location.image != event.image_tag:
            location.image = event.image_tag

        if event.k8s_config:
            location.set_k8s_overrides(event.k8s_config)

        location.save()

        Snapshot.create(
            location=location,
            image_tag=event.image_tag,
            content_json=event.snapshot_json,
            commit_hash=event.commit_hash,
        )

    logger.info("snapshot_persisted", location=event.location_name)
    deployments_total.labels(location=event.location_name).inc()


@app.get("/health/live")
def liveness_check():
    return {"status": "alive"}


@app.get("/health/ready")
def readiness_check():
    db_ok = False
    db_error = None

    try:
        with db_session():
            CodeLocation.select().limit(1).count()
        db_ok = True
    except Exception as e:
        db_error = str(e)

    status_code = 200 if db_ok else 503
    return {
        "status": "ready" if db_ok else "not ready",
        "checks": {
            "database": {"ok": db_ok, "error": db_error},
        },
    }, status_code


@app.get("/metrics")
def metrics():
    with db_session():
        locations_count.set(CodeLocation.select().count())
        snapshots_count.set(Snapshot.select().count())
    return Response(content=get_metrics_response(), media_type="text/plain")


@app.get("/health")
def health_check():
    db_ok = False
    db_error = None

    try:
        with db_session():
            CodeLocation.select().limit(1).count()
        db_ok = True
    except Exception as e:
        db_error = str(e)

    grpc_ok = grpc_server_instance is not None

    all_ok = db_ok and grpc_ok
    status_code = 200 if all_ok else 503

    return {
        "status": "healthy" if all_ok else "degraded",
        "version": __version__,
        "checks": {
            "database": {"ok": db_ok, "error": db_error},
            "grpc_proxy": {"ok": grpc_ok},
        },
    }, status_code


@app.post("/deploy", dependencies=[Depends(verify_token)])
async def receive_deployment(event: DeploymentEvent, background_tasks: BackgroundTasks):
    logger.info("deploy_received", location=event.location_name, image=event.image_tag)

    try:
        _register_deployment(event)
        return {"status": "success", "message": "Snapshot accepted"}

    except Exception as e:
        logger.error("deploy_failed", error=str(e))
        raise HTTPException(status_code=500, detail=str(e)) from e


@app.post("/webhooks/{backend_name}")
async def receive_webhook(backend_name: str, request: Request):
    logger.info("webhook_received", backend=backend_name)

    backend = backend_registry.get(backend_name) if backend_registry else None
    if not backend:
        raise HTTPException(status_code=404, detail=f"Backend '{backend_name}' not found")

    try:
        backend.validate_signature(request)
    except AuthenticationError as e:
        raise HTTPException(status_code=401, detail=str(e)) from e

    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON body") from None

    event = await backend.parse_event(body)
    if event is None:
        return {"status": "ignored", "message": "Event does not represent a completed deployment"}

    try:
        _register_deployment(event)
        return {"status": "success", "message": f"Webhook from '{backend_name}' processed"}
    except Exception as e:
        logger.error("webhook_register_failed", backend=backend_name, error=str(e))
        raise HTTPException(status_code=500, detail=str(e)) from e


@app.delete("/locations/{name}", dependencies=[Depends(verify_token)])
async def delete_location(name: str):
    logger.info("delete_location_request", location=name)

    try:
        with db_session():
            location = CodeLocation.get_or_none(CodeLocation.name == name)
            if not location:
                raise HTTPException(status_code=404, detail=f"Location '{name}' not found")

            deleted_snapshots = (
                Snapshot.delete().where(Snapshot.location == location).execute()
            )
            location.delete_instance()

        logger.info(
            "location_deleted",
            location=name,
            snapshots_removed=deleted_snapshots,
        )
        return {
            "status": "success",
            "message": f"Location '{name}' and {deleted_snapshots} snapshots deleted",
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error("delete_location_failed", error=str(e))
        raise HTTPException(status_code=500, detail=str(e)) from e


@app.post("/locations/{name}/rollback", dependencies=[Depends(verify_token)])
async def rollback_location(name: str):
    logger.info("rollback_location_request", location=name)

    try:
        with db_session():
            location = CodeLocation.get_or_none(CodeLocation.name == name)
            if not location:
                raise HTTPException(status_code=404, detail=f"Location '{name}' not found")

            latest = (
                Snapshot.select()
                .where(Snapshot.location == location)
                .order_by(Snapshot.created_at.desc())
                .first()
            )

            if not latest:
                raise HTTPException(status_code=404, detail=f"No snapshots found for '{name}'")

            previous = (
                Snapshot.select()
                .where(
                    Snapshot.location == location,
                    Snapshot.id != latest.id,
                )
                .order_by(Snapshot.created_at.desc())
                .first()
            )

            if not previous:
                raise HTTPException(
                    status_code=409,
                    detail=f"Cannot rollback: '{name}' has only one snapshot",
                )

            latest.delete_instance()

            location.image = previous.image_tag
            location.save()

        logger.info(
            "rollback_complete",
            location=name,
            removed_snapshot_id=latest.id,
            active_snapshot_id=previous.id,
            active_image=previous.image_tag,
        )
        return {
            "status": "success",
            "message": f"Rolled back '{name}' to snapshot {previous.id} ({previous.image_tag})",
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error("rollback_failed", error=str(e))
        raise HTTPException(status_code=500, detail=str(e)) from e
