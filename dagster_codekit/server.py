"""
HTTP server for dagster-codekit.

Receives webhooks from CI/CD tools and triggers the deployment engine.
Uses Starlette for a lightweight, async-native implementation.
"""

import structlog
from starlette.applications import Starlette
from starlette.middleware import Middleware
from starlette.middleware.cors import CORSMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route

from dagster_codekit.backends.argocd import ArgoCDBackend
from dagster_codekit.config import Config
from dagster_codekit.engine import DeploymentEngine
from dagster_codekit.exceptions import (
    AuthenticationError,
    ValidationError,
    ConfigurationError,
    TimeoutError,
)
from dagster_codekit.interfaces import BackendPlugin
from dagster_codekit.utils.reloader import DagsterReloader
from dagster_codekit.workspace import FileWorkspaceManager, K8sWorkspaceManager

logger = structlog.get_logger()


async def webhook_handler(request: Request) -> JSONResponse:
    """
    Handle incoming webhooks from CI/CD backends.
    Route: POST /webhooks/{backend}
    """
    backend_name = request.path_params.get("backend")

    # 1. Logger Context
    log = logger.bind(
        backend=backend_name,
        client_ip=request.client.host if request.client else "unknown",
        method=request.method,
    )

    # 2. Resolve Backend
    backends: dict[str, BackendPlugin] = request.app.state.backends
    if backend_name not in backends:
        log.warning("webhook_unknown_backend")
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
            log.warning("webhook_signature_invalid", error=str(e))
            return JSONResponse({"error": "Invalid signature"}, status_code=401)

        # 4. Parse Payload
        try:
            payload = await request.json()
            event = await backend.parse_event(payload)
        except ValidationError as e:
            log.warning("webhook_validation_failed", error=str(e))
            return JSONResponse({"error": str(e)}, status_code=400)
        except Exception as e:
            log.error("webhook_parse_error", error=str(e))
            return JSONResponse({"error": "Invalid payload format"}, status_code=400)

        # 5. Filter Ignored Events
        if event is None:
            log.debug("webhook_ignored", reason="Backend filtered event")
            return JSONResponse({"status": "ignored"}, status_code=200)

        # Update logger with event context
        log = log.bind(location=event.location_name, grpc_host=event.grpc_host)
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


def create_app(config: Config) -> Starlette:
    """Factory to create and configure the Starlette application."""

    logger.info("server_initializing", mode=config.workspace.mode)

    # A. Initialize Workspace Manager
    if config.workspace.mode == "file":
        manager = FileWorkspaceManager(config.workspace.file.get("path"))
    elif config.workspace.mode == "configmap":
        manager = K8sWorkspaceManager(
            namespace=config.workspace.configmap.get("namespace"),
            configmap_name=config.workspace.configmap.get("name"),
            max_retries=config.workspace.configmap.get("max_retries", 5),
        )
    else:
        raise ConfigurationError(f"Unknown workspace mode: {config.workspace.mode}")

    # B. Initialize Reloader
    reloader = DagsterReloader(
        webserver_url=config.dagster.webserver_url, auth_config=config.dagster.auth.model_dump()
    )

    # C. Initialize Engine
    engine = DeploymentEngine(workspace_manager=manager, reloader=reloader)

    # D. Initialize Backends
    backends: dict[str, BackendPlugin] = {}

    if config.argocd:
        backends["argocd"] = ArgoCDBackend(config.argocd.model_dump())
        logger.info("backend_enabled", name="argocd")

    if not backends:
        logger.warning("no_backends_configured")

    # E. Create App & State
    app = Starlette(
        debug=False,
        routes=[
            Route("/webhooks/{backend}", webhook_handler, methods=["POST"]),
            Route("/health", health_check, methods=["GET"]),
        ],
        middleware=[Middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"])],
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
