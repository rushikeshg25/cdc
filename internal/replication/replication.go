// Package replication manages the Postgres logical-replication connection: opening it
// in replication mode, creating/reusing a slot, streaming the CopyBoth WAL feed, and
// sending LSN feedback. Higher layers (decode, sink) consume what it produces.
package replication

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/rushikeshg25/cdc/internal/decode"
	"github.com/rushikeshg25/cdc/internal/sink"
)

// standbyTimeout is how often we proactively report our flushed LSN back to the server.
// Without this feedback Postgres would retain WAL indefinitely (and eventually drop us
// after wal_sender_timeout).
const standbyTimeout = 10 * time.Second

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

// Stream issues START_REPLICATION, which switches the socket into the bidirectional
// CopyBoth state, then loops receiving messages from the server. For now it just logs the
// kind of each message: keepalives ('k') and WAL data ('w'). Decoding the WAL payload and
// sending LSN feedback come in later commits.
//
// startLSN of 0 tells Postgres to resume from the slot's confirmed position.
func Stream(ctx context.Context, conn *pgconn.PgConn, slot, publication string, startLSN pglogrepl.LSN, snk sink.Sink) error {
	// pgoutput needs the protocol version and which publication's tables to stream.
	pluginArgs := []string{
		"proto_version '1'",
		fmt.Sprintf("publication_names '%s'", publication),
	}
	err := pglogrepl.StartReplication(ctx, conn, slot, startLSN,
		pglogrepl.StartReplicationOptions{PluginArgs: pluginArgs})
	if err != nil {
		return fmt.Errorf("START_REPLICATION: %w", err)
	}
	log.Printf("streaming slot=%s publication=%s from %s", slot, publication, startLSN)

	dec := decode.New()
	// clientXLogPos is the furthest WAL position we've processed; it's what we report back.
	clientXLogPos := startLSN
	nextStandbyDeadline := time.Now().Add(standbyTimeout)

	for {
		// Send periodic feedback so the server can free WAL up to clientXLogPos.
		// Flush the sink first so we only confirm an LSN whose events are durable.
		if time.Now().After(nextStandbyDeadline) {
			if err := flushAndReport(ctx, conn, snk, clientXLogPos); err != nil {
				return err
			}
			nextStandbyDeadline = time.Now().Add(standbyTimeout)
		}

		// Receive with a deadline so an idle stream still wakes us to send feedback.
		recvCtx, cancel := context.WithDeadline(ctx, nextStandbyDeadline)
		msg, err := conn.ReceiveMessage(recvCtx)
		cancel()
		if err != nil {
			if pgconn.Timeout(err) {
				continue // deadline hit: loop around and send feedback
			}
			if ctx.Err() != nil {
				// Signal-driven shutdown: flush the sink and report our final position.
				log.Printf("shutting down, flushing final position %s", clientXLogPos)
				if ferr := flushAndReport(context.Background(), conn, snk, clientXLogPos); ferr != nil {
					log.Printf("final flush/feedback failed: %v", ferr)
				}
				return nil
			}
			return fmt.Errorf("receive message: %w", err)
		}

		cd, ok := msg.(*pgproto3.CopyData)
		if !ok {
			log.Printf("unexpected message %T", msg)
			continue
		}

		switch cd.Data[0] {
		case pglogrepl.PrimaryKeepaliveMessageByteID:
			pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(cd.Data[1:])
			if err != nil {
				return fmt.Errorf("parse keepalive: %w", err)
			}
			// ReplyRequested means the server wants our position now, not on the timer.
			if pkm.ReplyRequested {
				nextStandbyDeadline = time.Time{}
			}

		case pglogrepl.XLogDataByteID:
			xld, err := pglogrepl.ParseXLogData(cd.Data[1:])
			if err != nil {
				return fmt.Errorf("parse XLogData: %w", err)
			}
			events, err := dec.Process(xld.WALStart, xld.WALData)
			if err != nil {
				return err
			}
			for _, ev := range events {
				if err := snk.Write(ev); err != nil {
					return fmt.Errorf("sink write: %w", err)
				}
			}
			// Advance past the bytes we just consumed.
			clientXLogPos = xld.WALStart + pglogrepl.LSN(len(xld.WALData))

		default:
			log.Printf("unknown CopyData kind %q", cd.Data[0])
		}
	}
}

// flushAndReport makes the sink durable up to pos, then reports pos to the server as
// write/flush/apply. Flushing before reporting is the at-least-once guarantee: we never
// tell Postgres it can recycle WAL for events we haven't persisted.
func flushAndReport(ctx context.Context, conn *pgconn.PgConn, snk sink.Sink, pos pglogrepl.LSN) error {
	if err := snk.Flush(); err != nil {
		return fmt.Errorf("sink flush: %w", err)
	}
	err := pglogrepl.SendStandbyStatusUpdate(ctx, conn,
		pglogrepl.StandbyStatusUpdate{WALWritePosition: pos})
	if err != nil {
		return fmt.Errorf("send standby status update: %w", err)
	}
	log.Printf("flushed sink + reported LSN %s", pos)
	return nil
}
