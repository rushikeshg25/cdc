// Package replication manages the Postgres logical-replication connection: opening it
// in replication mode, creating/reusing a slot, streaming the CopyBoth WAL feed, and
// sending LSN feedback. Higher layers (decode, sink) consume what it produces.
package replication

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgconn"
)

// Connect opens a Postgres connection in *replication mode*. A normal connection speaks
// SQL; setting the replication=database runtime parameter switches it into the streaming
// replication sub-protocol that IDENTIFY_SYSTEM / CREATE_REPLICATION_SLOT /
// START_REPLICATION require.
func Connect(ctx context.Context, dsn string) (*pgconn.PgConn, error) {
	cfg, err := pgconn.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse dsn: %w", err)
	}
	// This is the switch that makes it a replication connection rather than a SQL one.
	cfg.RuntimeParams["replication"] = "database"

	conn, err := pgconn.ConnectConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("connect (replication mode): %w", err)
	}
	return conn, nil
}
