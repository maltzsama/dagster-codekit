from playhouse.db_url import connect
from .models import db_proxy, CodeLocation, Snapshot

def initialize_database(db_url: str):
    """
    Initializes the database connection and creates tables.
    db_url can be:
      - sqlite:///./codekit.db
      - postgres://user:password@localhost:5432/dbname
    """
    database = connect(db_url)
    db_proxy.initialize(database)
    
    # Ensure tables exist
    database.create_tables([CodeLocation, Snapshot], safe=True)
    return database
