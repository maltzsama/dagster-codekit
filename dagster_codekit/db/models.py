import datetime
import structlog
from peewee import (
    Model, CharField, TextField, DateTimeField, ForeignKeyField, 
    DatabaseProxy
)
from playhouse.db_url import connect

logger = structlog.get_logger(__name__)

# O Proxy permite definir o banco (SQLite/Postgres) em tempo de execução
db = DatabaseProxy()

class BaseModel(Model):
    class Meta:
        database = db

class CodeLocation(BaseModel):
    """
    Representa um repositório lógico (ex: 'data_platform').
    """
    name = CharField(unique=True, index=True)
    description = TextField(null=True)
    image = CharField(help_text="Imagem Docker padrão/atual")
    namespace = CharField(default="dagster")
    created_at = DateTimeField(default=datetime.datetime.utcnow)
    updated_at = DateTimeField(default=datetime.datetime.utcnow)

class Snapshot(BaseModel):
    """
    Histórico de deploys. Contém o JSON gordo do Dagster.
    """
    location = ForeignKeyField(CodeLocation, backref="snapshots")
    content_json = TextField()
    image_tag = CharField()
    commit_hash = CharField(null=True)
    created_at = DateTimeField(default=datetime.datetime.utcnow)

    class Meta:
        indexes = (
            (('location', 'image_tag'), False),
        )

def init_db(url: str):
    """
    Inicializa a conexão e cria as tabelas se não existirem.
    Chamado pelo app.py no startup.
    """
    try:

        database = connect(url)
        db.initialize(database)


        if not db.is_closed():
            db.close()
        db.connect()


        db.create_tables([CodeLocation, Snapshot], safe=True)
        logger.info("database_initialized", url=url)
        
    except Exception as e:
        logger.error("database_init_failed", error=str(e))
        raise