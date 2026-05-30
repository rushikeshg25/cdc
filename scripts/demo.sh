#!/usr/bin/env bash
#
# Drive a scripted insert/update/delete/truncate workload to watch the CDC engine emit
# events. Run `cdc` in one terminal, then this in another.
#
# Targets the Dockerized Postgres by default; override with PSQL to point elsewhere, e.g.
#   PSQL="psql postgres://cdc:cdc@localhost:5433/cdc_demo" ./scripts/demo.sh
set -euo pipefail

PSQL="${PSQL:-docker exec -i cdc_postgres psql -U cdc -d cdc_demo}"

run() { echo "  $1"; $PSQL -v ON_ERROR_STOP=1 -q -c "$1"; }

echo "==> Setting up demo.products (REPLICA IDENTITY FULL for full before-images)"
$PSQL -v ON_ERROR_STOP=1 -q <<'SQL'
CREATE TABLE IF NOT EXISTS products (
  id    serial PRIMARY KEY,
  name  text NOT NULL,
  price numeric(10,2) NOT NULL,
  tags  jsonb
);
ALTER TABLE products REPLICA IDENTITY FULL;
TRUNCATE products;
SQL

echo "==> INSERTs"
run "INSERT INTO products(name, price, tags) VALUES ('widget', 9.99, '{\"color\":\"red\"}');"
run "INSERT INTO products(name, price, tags) VALUES ('gadget', 19.50, '{\"color\":\"blue\"}');"
run "INSERT INTO products(name, price) VALUES ('gizmo', 4.25);"

echo "==> UPDATEs"
run "UPDATE products SET price = 8.99 WHERE name = 'widget';"
run "UPDATE products SET tags = '{\"color\":\"green\",\"sale\":true}' WHERE name = 'gadget';"

echo "==> DELETE"
run "DELETE FROM products WHERE name = 'gizmo';"

echo "==> TRUNCATE"
run "TRUNCATE products;"

echo
echo "Done. Check your sink (e.g. tail -f events.jsonl) for insert/update/delete/truncate events."
