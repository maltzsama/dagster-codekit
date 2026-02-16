"""
HTTP server for dagster-codekit.

Receives webhooks from CI/CD tools and triggers the deployment engine.
Uses Starlette for a lightweight, async-native implementation.
"""

import uuid

import structlog
from starlette.applications import Starlette
from starlette.middleware import Middleware
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.middleware.cors import CORSMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route
from structlog.contextvars import bind_contextvars
from structlog.contextvars import clear_contextvars

from dagster_codekit.backends.argocd import ArgoCDBackend
from dagster_codekit.config import Config
from dagster_codekit.core.engine import DeploymentEngine
from dagster_codekit.core.exceptions import (
    AuthenticationError,
    ValidationError,
    ConfigurationError,
    TimeoutError,
)
from dagster_codekit.core.interfaces import BackendPlugin
from dagster_codekit.utils.reloader import DagsterReloader
from dagster_codekit.workspace import FileWorkspaceManager, K8sWorkspaceManager
from contextlib import asynccontextmanager

logger = structlog.get_logger()


class LoggingContextMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request, call_next):
        clear_contextvars()
        request_id = str(uuid.uuid4())

        bind_contextvars(
            request_id=request_id,
            method=request.method,
            path=request.url.path,
            client_ip=request.client.host if request.client else "unknown",
        )

        try:
            response = await call_next(request)
            response.headers["X-Request-ID"] = request_id
            return response
        except Exception as e:
            logger.exception("unhandled_server_exception", error=str(e))
            return JSONResponse(
                {"error": "Internal Server Error", "request_id": request_id}, status_code=500
            )
        finally:
            clear_contextvars()


async def webhook_handler(request: Request) -> JSONResponse:
    """
    Handle incoming webhooks from CI/CD backends.
    Route: POST /webhooks/{backend}
    """
    clear_contextvars()

    backend_name = request.path_params.get("backend")

    bind_contextvars(
        backend=backend_name, client_ip=request.client.host if request.client else "unknown"
    )

    # 2. Resolve Backend
    backends: dict[str, BackendPlugin] = request.app.state.backends
    if backend_name not in backends:
        logger.warning("webhook_unknown_backend")
        return JSONResponse(
            {"error": f"Backend '{backend_name}' not configured or disabled"}, status_code=404
        )

    backend = backends[backend_name]
    engine: DeploymentEngine = request.app.state.engine

    try:
        # 3. Validate Signature (Security First)
        try:
            backend.validate_signature(request)
        except AuthenticationError as e:
            logger.warning("webhook_signature_invalid", error=str(e))
            return JSONResponse({"error": "Invalid signature"}, status_code=401)

        # 4. Parse Payload
        try:
            payload = await request.json()
            event = await backend.parse_event(payload)
        except ValidationError as e:
            logger.warning("webhook_validation_failed", error=str(e))
            return JSONResponse({"error": str(e)}, status_code=400)
        except Exception as e:
            logger.error("webhook_parse_error", error=str(e))
            return JSONResponse({"error": "Invalid payload format"}, status_code=400)

        # 5. Filter Ignored Events
        if event is None:
            logger.debug("webhook_ignored", reason="Backend filtered event")
            return JSONResponse({"status": "ignored"}, status_code=200)

        # Update logger with event context
        log = logger.bind(location=event.location_name, grpc_host=event.grpc_host)
        log.info("webhook_event_accepted")

        # 6. Wait for Deployment Readiness (Health Check)
        try:
            is_ready = await backend.wait_ready(event)
            if not is_ready:
                log.error("deployment_not_ready")
                return JSONResponse(
                    {"error": "Deployment health check failed/timeout"}, status_code=504
                )
        except TimeoutError as e:
            log.error("deployment_timeout", error=str(e))
            return JSONResponse({"error": str(e)}, status_code=504)

        # 7. Process Deployment (Engine)
        await engine.process_deployment(event)

        return JSONResponse(
            {
                "status": "success",
                "location": event.location_name,
                "message": "Deployment processed and workspace reloaded",
            },
            status_code=200,
        )

    except Exception as e:
        log.error("webhook_processing_failed", error=str(e), exc_info=True)
        return JSONResponse({"error": "Internal Server Error"}, status_code=500)


async def health_check(request: Request) -> JSONResponse:
    """Simple liveness probe."""
    return JSONResponse({"status": "healthy", "backends": list(request.app.state.backends.keys())})


@asynccontextmanager
async def lifespan(app: Starlette):
    """
    Service startup and shutdown logic.
    Runs pre-flight checks to ensure dependencies are alive.
    """
    config = app.state.config
    engine = app.state.engine

    logger.info("startup_checks_started")

    # 1. Validate Workspace Access
    try:
        await engine.workspace_manager.validate()
        logger.info("startup_check_workspace", status="ok")
    except Exception as e:
        logger.error("startup_check_workspace_failed", error=str(e))
        # Hard fail: if we can't write to the workspace, the tool is useless
        raise SystemExit(1)

    # 2. Check Dagster Connectivity
    is_dagster_up = await engine.reloader.check_connection()
    if not is_dagster_up:
        logger.warning(
            "startup_check_dagster_failed",
            help="Dagster Webserver is unreachable. Deploys will fail until it's back.",
        )

    logger.info("startup_checks_completed", status="ready")
    yield
    logger.info("server_shutting_down")


def create_app(config: Config) -> Starlette:
    """Factory to create and configure the Starlette application."""

    logger.info("server_initializing", mode=config.workspace.mode)

    # A. Initialize Workspace Manager
    if config.workspace.mode == "file":
        manager = FileWorkspaceManager(config.workspace.file.path)
    elif config.workspace.mode == "configmap":
        manager = K8sWorkspaceManager(
            namespace=config.workspace.configmap.namespace,
            configmap_name=config.workspace.configmap.name,
            max_retries=config.workspace.configmap.max_retries,
        )
    else:
        raise ConfigurationError(f"Unknown workspace mode: {config.workspace.mode}")

    # B. Initialize Reloader
    reloader = DagsterReloader(
        webserver_url=config.dagster.webserver_url, auth_config=config.dagster.auth
    )

    # C. Initialize Engine
    engine = DeploymentEngine(workspace_manager=manager, reloader=reloader)

    # D. Initialize Backends
    backends: dict[str, BackendPlugin] = {}

    if config.argocd:
        backends["argocd"] = ArgoCDBackend(config.argocd)
        logger.info("backend_enabled", name="argocd")

    if not backends:
        logger.warning("no_backends_configured")

    # E. Create App & State
    app = Starlette(
        debug=False,
        lifespan=lifespan,
        routes=[
            Route("/webhooks/{backend}", webhook_handler, methods=["POST"]),
            Route("/health", health_check, methods=["GET"]),
        ],
        middleware=[
            Middleware(LoggingContextMiddleware),
            Middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"]),
        ],
    )

    # Store components in state for access in routes
    app.state.config = config
    app.state.engine = engine
    app.state.backends = backends

    return app


async def run_server(config: Config):
    """Entrypoint called by CLI."""
    import uvicorn

    app = create_app(config)

    log_config = uvicorn.config.LOGGING_CONFIG
    log_config["formatters"]["access"][
        "fmt"
    ] = "%(asctime)s - %(client_addr)s - '%(request_line)s' %(status_code)s"

    config_uvicorn = uvicorn.Config(
        app, host=config.server.host, port=config.server.port, log_level="info", access_log=True
    )

    server = uvicorn.Server(config_uvicorn)
    await server.serve()
