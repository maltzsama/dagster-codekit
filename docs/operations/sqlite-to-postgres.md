# Migrating from SQLite to Postgres

This guide covers migrating an existing CodeKit SQLite database to Postgres,
preserving all code location registrations and snapshot history.

## Why migrate?

SQLite is safe for single-replica/local use only. When scaling the proxy beyond
one replica (e.g., setting `spec.replicas > 1` in Kubernetes), each pod would
have its own local filesystem, leading to divergent snapshot history per pod.
Postgres provides a shared, multi-replica-safe database backend.

## Procedure

### 1. Export data from SQLite

With your current SQLite-based CodeKit running:

```bash
dagster-codekit db export --config config.yaml --output codekit_export.jsonl
```

This produces a JSON-lines file containing all `CodeLocation` and `Snapshot` rows.

### 2. Prepare Postgres

Create a Postgres database:

```sql
CREATE DATABASE codekit;
CREATE USER codekit WITH PASSWORD '<password>';
GRANT ALL PRIVILEGES ON DATABASE codekit TO codekit;
```

### 3. Run migrations against Postgres

Create a temporary `config.postgres.yaml`:

```yaml
database:
  url: "postgresql://codekit:<password>@<host>:5432/codekit"
```

Then:

```bash
dagster-codekit db migrate --config config.postgres.yaml
```

### 4. Import data into Postgres

```bash
dagster-codekit db import --config config.postgres.yaml --input codekit_export.jsonl
```

### 5. Switch CodeKit to Postgres

Update your main `config.yaml`:

```yaml
database:
  url: "postgresql://codekit:<password>@<host>:5432/codekit"
```

Restart CodeKit. You can now safely scale beyond one replica.

## Verification

After migration, verify row counts match:

```bash
# SQLite
sqlite3 codekit.db "SELECT COUNT(*) FROM codelocation; SELECT COUNT(*) FROM snapshot;"

# Postgres
psql -U codekit -c "SELECT COUNT(*) FROM codelocation; SELECT COUNT(*) FROM snapshot;"
```
