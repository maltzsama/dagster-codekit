import structlog
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Header, Depends, BackgroundTasks
from pydantic import ValidationError

from dagster_codekit.config import load_config, Config
from dagster_codekit.core.grpc_proxy import run_grpc_server
from dagster_codekit.core.engine import create_snapshot_payload
from dagster_codekit.api.schemas import DeploymentEvent
from dagster_codekit.db.models import init_db, Snapshot, CodeLocation

logger = structlog.get_logger(__name__)

# Carrega config global
cfg = load_config("config.yaml")

# Estado global para segurar a thread do servidor gRPC
grpc_server_instance = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    # --- STARTUP ---
    logger.info("startup", msg="Initializing Codekit...")
    
    # 1. Inicializa DB
    init_db(cfg.database.url)
    
    # 2. Inicia o gRPC Proxy em uma Thread separada
    # Passamos None como db_conn se o proxy usar o ORM global, 
    # ou passamos a conexão se necessário. O ORM Peewee é global.
    global grpc_server_instance
    grpc_server_instance = run_grpc_server(
        host="0.0.0.0", # gRPC ouve em todas as interfaces
        port=cfg.server.grpc_port,
        db_conn=None, # O Proxy usa os models globais
        max_workers=cfg.server.workers
    )
    
    yield
    
    # --- SHUTDOWN ---
    logger.info("shutdown", msg="Stopping Codekit...")
    if grpc_server_instance:
        grpc_server_instance.stop(grace=5)

app = FastAPI(title="Dagster Codekit", version="1.0.0", lifespan=lifespan)

# --- AUTH DEPENDENCY ---
def verify_token(authorization: str = Header(None)):
    if not cfg.auth.enabled:
        return True
    
    if not authorization:
        raise HTTPException(status_code=401, detail="Missing Authorization Header")
    
    try:
        scheme, token = authorization.split()
        if scheme.lower() != 'bearer':
            raise HTTPException(status_code=401, detail="Invalid Auth Scheme")
        if token not in cfg.auth.tokens:
            raise HTTPException(status_code=403, detail="Invalid Token")
    except ValueError:
        raise HTTPException(status_code=401, detail="Invalid Header Format")

# --- ROUTES ---

@app.get("/health")
def health_check():
    return {"status": "ok", "version": "1.0.0"}

@app.post("/deploy", dependencies=[Depends(verify_token)])
async def receive_deployment(event: DeploymentEvent, background_tasks: BackgroundTasks):
    """
    Recebe o Snapshot do CI/CD e salva no banco.
    """
    logger.info("deploy_received", location=event.location_name, image=event.image_tag)
    
    try:
        # 1. Garante que a Location existe
        location, _ = CodeLocation.get_or_create(
            name=event.location_name,
            defaults={"image": event.image_tag, "namespace": cfg.launcher.k8s.namespace}
        )
        
        # 2. Atualiza a imagem padrão da location
        if location.image != event.image_tag:
            location.image = event.image_tag
            location.save()

        # 3. Salva o Snapshot
        Snapshot.create(
            location=location,
            image_tag=event.image_tag,
            content_json=event.snapshot_json,
            git_hash=event.commit_hash
        )
        
        logger.info("snapshot_persisted", location=event.location_name)
        return {"status": "success", "message": "Snapshot accepted"}

    except Exception as e:
        logger.error("deploy_failed", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))