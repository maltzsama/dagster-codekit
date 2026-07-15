# Operations Runbook

## Backup

### SQLite

SQLite uses WAL mode. Before copying the file, flush the WAL:

```bash
sqlite3 /opt/dagster/codekit_data/codekit.db "PRAGMA wal_checkpoint(TRUNCATE);"
cp /opt/dagster/codekit_data/codekit.db /backups/codekit-$(date +%Y%m%d).db
```

### Postgres

Standard `pg_dump`:

```bash
pg_dump -U codekit -h <host> codekit > /backups/codekit-$(date +%Y%m%d).sql
```

## Restore

### SQLite

```bash
# Stop CodeKit first
cp /backups/codekit-20250101.db /opt/dagster/codekit_data/codekit.db
# Start CodeKit
```

### Postgres

```bash
# Drop and recreate
psql -U codekit -h <host> -c "DROP DATABASE codekit;"
psql -U codekit -h <host> -c "CREATE DATABASE codekit;"

# Restore, then run migrations
psql -U codekit -h <host> codekit < /backups/codekit-20250101.sql
dagster-codekit db migrate --config config.yaml
```

**Important:** Pause CI/CD deploys during the restore window. Deploys that happen
against the old database during the backup will not be in the backup file.

## gRPC proxy is down

If the Dagster webserver reports `DagsterUserCodeUnreachableError` for a code
location served by CodeKit:

1. Check CodeKit's health:
   ```bash
   curl http://codekit:8000/health
   ```
   A healthy response includes `database` and `grpc_proxy` checks.

2. Check gRPC connectivity from the webserver pod:
   ```bash
   kubectl exec -it deploy/dagster-webserver -- grpcurl -plaintext codekit:4000 dagster_api.DagsterApi/Ping
   ```

3. If the proxy is healthy but the webserver can't reach it, check:
   - Network policies allowing port 4000 between webserver and codekit namespaces
   - DNS resolution of the codekit Service name from the webserver pod

4. Safe restart procedure (no data loss — all state is in the database):
   ```bash
   kubectl rollout restart deployment/codekit -n dagster
   ```

## A run is stuck

If a launched K8s Job is stuck (CancelExecution in the UI failed or K8s API was
unavailable when cancellation was attempted):

1. Identify the stuck job:
   ```bash
   kubectl get jobs -n dagster -l dagster/run-id=<run-id>
   ```

2. Manual cleanup (last resort — try CancelExecution first):
   ```bash
   kubectl delete job dagster-run-<run-id> -n dagster
   ```

   This deletes the Job and its associated Pods. The Dagster instance will
   see the run as `CANCELED` after the next status poll.

## Rolling out a schema migration

When upgrading CodeKit to a version that introduces a new database migration:

1. **Before rollout:** Run migrations explicitly (avoids N replicas racing):
   ```bash
   dagster-codekit db migrate --config config.yaml
   ```

2. **If a migration fails partway:** peewee-migrate runs each migration in a
   transaction. A failed migration rolls back to the last successful state.
   Check logs for the specific error, fix the issue (e.g., missing dependency),
   and re-run.

3. **Rolling back a migration:**
   ```bash
   # List applied migrations
   ls migrations/

   # Rollback the last migration
   dagster-codekit db rollback --config config.yaml
   ```

## Database health check

The `/health/ready` endpoint verifies database connectivity. Monitor this in
your pod's readiness probe:

```yaml
readinessProbe:
  httpGet:
    path: /health/ready
    port: 8000
  initialDelaySeconds: 5
  periodSeconds: 10
```

If `/health/ready` returns 503, the database is unreachable — the pod will
be removed from the Service and stop receiving traffic until connectivity
is restored.
