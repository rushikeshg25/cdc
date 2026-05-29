#!/usr/bin/env bash
#
# Prepare a local Homebrew PostgreSQL 17 for logical-replication CDC.
#
# What it does:
#   1. Enables logical replication via ALTER SYSTEM (wal_level + sender/slot limits).
#   2. Restarts Postgres (wal_level changes require a full restart, not a reload).
#   3. Creates the dev database.
#   4. Creates a publication covering the tables we want to capture.
#
# The replication *slot* is created by the cdc binary itself (idempotently), so it is
# not created here.
#
# Override any of these via the environment:
set -euo pipefail

PG_SERVICE="${PG_SERVICE:-postgresql@17}"
PGBIN="${PGBIN:-/opt/homebrew/opt/postgresql@17/bin}"
DB_NAME="${DB_NAME:-cdc_demo}"
PUBLICATION="${PUBLICATION:-cdc_pub}"
# Admin connection used to ALTER SYSTEM / CREATE DATABASE. Defaults to the local
# superuser (your macOS user) on the default 'postgres' maintenance database.
ADMIN_DB="${ADMIN_DB:-postgres}"

PSQL="${PGBIN}/psql"

echo "==> Enabling logical replication (ALTER SYSTEM)"
"$PSQL" -d "$ADMIN_DB" -v ON_ERROR_STOP=1 <<'SQL'
ALTER SYSTEM SET wal_level = 'logical';
ALTER SYSTEM SET max_wal_senders = 10;
ALTER SYSTEM SET max_replication_slots = 10;
SQL

echo "==> Restarting ${PG_SERVICE} (required for wal_level change)"
brew services restart "$PG_SERVICE"

echo "==> Waiting for Postgres to accept connections"
for i in $(seq 1 30); do
  if "$PGBIN/pg_isready" -q; then break; fi
  sleep 0.5
done
"$PGBIN/pg_isready"

echo "==> Verifying wal_level"
"$PSQL" -d "$ADMIN_DB" -tAc "SHOW wal_level;"

echo "==> Creating database '${DB_NAME}' (if absent)"
if ! "$PSQL" -d "$ADMIN_DB" -tAc "SELECT 1 FROM pg_database WHERE datname='${DB_NAME}';" | grep -q 1; then
  "$PSQL" -d "$ADMIN_DB" -v ON_ERROR_STOP=1 -c "CREATE DATABASE ${DB_NAME};"
else
  echo "    already exists"
fi

echo "==> Creating publication '${PUBLICATION}' FOR ALL TABLES (if absent)"
"$PSQL" -d "$DB_NAME" -v ON_ERROR_STOP=1 <<SQL
DO \$\$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = '${PUBLICATION}') THEN
    EXECUTE 'CREATE PUBLICATION ${PUBLICATION} FOR ALL TABLES';
  END IF;
END
\$\$;
SQL

echo
echo "Done. Database='${DB_NAME}' publication='${PUBLICATION}'."
echo "Tip: for full before-images on UPDATE/DELETE, set:"
echo "     ALTER TABLE <table> REPLICA IDENTITY FULL;"
