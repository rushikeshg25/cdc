#!/usr/bin/env bash
#
# Bring up the Dockerized Postgres 17 for CDC. Unlike setup_pg.sh, nothing needs to be
# enabled or restarted: wal_level=logical is set via server flags in docker-compose.yml,
# and the publication is created by scripts/init_pg.sql on first init.
#
# Connection string for the cdc binary:
#   postgres://cdc:cdc@localhost:5433/cdc_demo?sslmode=disable
set -euo pipefail

echo "==> Starting Postgres container"
docker compose up -d

echo "==> Waiting for healthcheck"
for i in $(seq 1 30); do
  status="$(docker inspect -f '{{.State.Health.Status}}' cdc_postgres 2>/dev/null || echo starting)"
  if [ "$status" = "healthy" ]; then break; fi
  sleep 1
done
docker inspect -f 'health: {{.State.Health.Status}}' cdc_postgres

echo
echo "Ready. Connect with: postgres://cdc:cdc@localhost:5433/cdc_demo?sslmode=disable"
echo "Tip: for full before-images on UPDATE/DELETE, set:"
echo "     ALTER TABLE <table> REPLICA IDENTITY FULL;"
