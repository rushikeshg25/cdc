// Package replication manages the Postgres logical-replication connection: opening it
// in replication mode, creating/reusing a slot, streaming the CopyBoth WAL feed, and
// sending LSN feedback. Higher layers (decode, sink) consume what it produces.
package replication

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
)

// outputPlugin is the logical decoding plugin we stream through. pgoutput is built in.
const outputPlugin = "pgoutput"

// pgErrDuplicateObject is raised when the slot already exists (duplicate_object).
const pgErrDuplicateObject = "42710"

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

// IdentifySystem runs the IDENTIFY_SYSTEM replication command, which reports the server's
// system identifier, current timeline, current WAL position (LSN), and database name. It's
// a cheap way to confirm the replication connection works and to see where the WAL is now.
func IdentifySystem(ctx context.Context, conn *pgconn.PgConn) (pglogrepl.IdentifySystemResult, error) {
	sys, err := pglogrepl.IdentifySystem(ctx, conn)
	if err != nil {
		return pglogrepl.IdentifySystemResult{}, fmt.Errorf("IDENTIFY_SYSTEM: %w", err)
	}
	return sys, nil
}

// EnsureSlot creates a persistent logical replication slot using the pgoutput plugin, or
// leaves it in place if it already exists. The slot is the server-side bookmark that keeps
// WAL around until we confirm we've processed it. It returns true if it created the slot.
func EnsureSlot(ctx context.Context, conn *pgconn.PgConn, slotName string) (created bool, err error) {
	_, err = pglogrepl.CreateReplicationSlot(ctx, conn, slotName, outputPlugin,
		pglogrepl.CreateReplicationSlotOptions{Temporary: false})
	if err == nil {
		return true, nil
	}

	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == pgErrDuplicateObject {
		// Slot already exists from a previous run — reuse it.
		return false, nil
	}
	return false, fmt.Errorf("create replication slot %q: %w", slotName, err)
}
