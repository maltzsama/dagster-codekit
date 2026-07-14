import json

import datetime
from contextlib import contextmanager
from urllib.parse import urlparse

import structlog
from peewee import (
    Model,
    CharField,
    TextField,
    DateTimeField,
    ForeignKeyField,
    DatabaseProxy,
)
from playhouse.pool import PooledSqliteDatabase, PooledPostgresqlExtDatabase

logger = structlog.get_logger(__name__)

db = DatabaseProxy()


class BaseModel(Model):
    class Meta:
        database = db


class CodeLocation(BaseModel):
    name = CharField(unique=True, index=True)
    description = TextField(null=True)
    image = CharField(help_text="Imagem Docker padrão/atual")
    namespace = CharField(default="dagster")
    k8s_config = TextField(null=True, help_text="JSON with per-location K8s overrides")
    created_at = DateTimeField(default=datetime.datetime.utcnow)
    updated_at = DateTimeField(default=datetime.datetime.utcnow)

    def get_k8s_overrides(self) -> dict:
        if self.k8s_config:
            try:
                return json.loads(self.k8s_config)
            except (json.JSONDecodeError, TypeError):
                return {}
        return {}

    def set_k8s_overrides(self, overrides: dict) -> None:
        self.k8s_config = json.dumps(overrides) if overrides else None


class Snapshot(BaseModel):
    location = ForeignKeyField(CodeLocation, backref="snapshots")
    content_json = TextField()
    image_tag = CharField()
    commit_hash = CharField(null=True)
    created_at = DateTimeField(default=datetime.datetime.utcnow)

    class Meta:
        indexes = ((("location", "image_tag"), False),)


def _create_pooled_db(url: str):
    """Create a thread-safe pooled database from a connection URL."""
    parsed = urlparse(url)
    scheme = parsed.scheme

    if scheme == "sqlite":
        db_path = parsed.path[1:] if parsed.path else ":memory:"
        return PooledSqliteDatabase(
            db_path,
            pragmas={"journal_mode": "wal", "foreign_keys": "on"},
            max_connections=32,
            stale_timeout=300,
            check_same_thread=False,
        )
    elif scheme in ("postgresql", "postgres"):
        return PooledPostgresqlExtDatabase(
            parsed.path.lstrip("/") if parsed.path else "codekit",
            user=parsed.username,
            password=parsed.password,
            host=parsed.hostname or "localhost",
            port=parsed.port or 5432,
            max_connections=32,
            stale_timeout=300,
        )
    else:
        raise ValueError(f"Unsupported database URL scheme: {scheme}")


def init_db(url: str):
    """Initialize the database connection pool and create tables."""
    try:
        database = _create_pooled_db(url)
        db.initialize(database)

        with db.connection_context():
            db.create_tables([CodeLocation, Snapshot], safe=True)

        logger.info("database_initialized", url=url)
    except Exception as e:
        logger.error("database_init_failed", error=str(e))
        raise


@contextmanager
def db_session():
    """Context manager that provides a thread-safe database connection from the pool."""
    with db.connection_context():
        yield