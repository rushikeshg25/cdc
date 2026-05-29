-- Runs once on first container init (mounted into /docker-entrypoint-initdb.d/).
-- Executes against POSTGRES_DB (cdc_demo). wal_level is already set via server flags
-- in docker-compose.yml, so we only need the publication here. The replication slot is
-- created by the cdc binary itself.
CREATE PUBLICATION cdc_pub FOR ALL TABLES;
