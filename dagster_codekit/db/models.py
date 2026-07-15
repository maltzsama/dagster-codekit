import datetime
import json
import os
from contextlib import contextmanager
from urllib.parse import urlparse

import structlog
from peewee import (
    CharField,
    DatabaseProxy,
    DateTimeField,
    ForeignKeyField,
    Model,
    TextField,
)
from playhouse.pool import PooledSqliteDatabase

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
        try:
            from playhouse.pool import PooledPostgresqlExtDatabase as PgPool
        except ImportError:
            from playhouse.pool import PooledPostgresqlDatabase as PgPool

        return PgPool(
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
    """Initialize the database connection pool and run migrations."""
    try:
        _check_sqlite_replica_safety(url)

        database = _create_pooled_db(url)
        db.initialize(database)
        run_migrations()
        logger.info("database_initialized", url=url)
    except Exception as e:
        logger.error("database_init_failed", error=str(e))
        raise


def _check_sqlite_replica_safety(url: str):
    if not url.startswith("sqlite"):
        return

    replica_count = int(os.environ.get("CODEKIT_REPLICA_COUNT", "0"))
    if replica_count > 1:
        raise RuntimeError(
            "SQLite is not safe with multiple proxy replicas (CODEKIT_REPLICA_COUNT > 1). "
            "Use the 'postgres' extra and set database.url to a Postgres connection string. "
            "See docs/operations/sqlite-to-postgres.md for migration instructions."
        )

    if replica_count == 0:
        logger.warning(
            "sqlite_single_replica_warning",
            msg="SQLite is configured. This is safe for single-replica/local use only. "
                "Set CODEKIT_REPLICA_COUNT env var if running in Kubernetes.",
        )


def run_migrations():
    """Apply pending schema migrations."""
    from peewee_migrate import Router

    migrations_dir = os.environ.get(
        "CODEKIT_MIGRATIONS_DIR",
        os.path.join(os.path.dirname(__file__), "..", "..", "migrations"),
    )
    migrations_dir = os.path.abspath(migrations_dir)

    router = Router(db, migrate_dir=migrations_dir)

    if not router.todo:
        logger.info("no_pending_migrations")
        return

    auto = os.environ.get("CODEKIT_AUTO_CREATE_MIGRATION", "0") == "1"
    if auto and not router.done:
        router.create(auto=[CodeLocation, Snapshot])

    migrations = router.run()
    logger.info("migrations_applied", count=len(migrations), names=migrations)


@contextmanager
def db_session():
    """Context manager that provides a thread-safe database connection from the pool."""
    with db.connection_context():
        yield
