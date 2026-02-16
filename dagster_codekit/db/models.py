"""
Core data models for dagster-codekit.
These are the contracts between different components.
"""

from peewee import *
import datetime


db_proxy = Proxy()

class BaseModel(Model):
    class Meta:
        database = db_proxy

class CodeLocation(BaseModel):
    name = CharField(unique=True)
    description = TextField(null=True)
    image = CharField() 
    namespace = CharField(default="dagster")
    updated_at = DateTimeField(default=datetime.datetime.now)

class Snapshot(BaseModel):
    location = ForeignKeyField(CodeLocation, backref='snapshots')
    content_json = TextField()
    image_tag = CharField()
    git_hash = CharField(null=True)
    created_at = DateTimeField(default=datetime.datetime.now)


