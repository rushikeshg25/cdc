// Package replication manages the Postgres logical-replication connection: opening it
// in replication mode, creating/reusing a slot, streaming the CopyBoth WAL feed, and
// sending LSN feedback. Higher layers (decode, sink) consume what it produces.
package replication

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/rushikeshg25/cdc/internal/decode"
	"github.com/rushikeshg25/cdc/internal/metrics"
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

// SlotInfo describes the result of ensuring a replication slot exists.
type SlotInfo struct {
	// Created is true when this call created the slot (vs. reusing an existing one).
	Created bool
	// ConsistentPoint is the LSN at which the slot was created — the exact point from which
	// streaming resumes everything that happened after the exported snapshot. Only
	// meaningful when Created is true.
	ConsistentPoint pglogrepl.LSN
	// SnapshotName is the exported snapshot a separate connection can import to read a
	// consistent view of existing rows. Only set when Created is true.
	SnapshotName string
}

// EnsureSlot creates a persistent logical replication slot using the pgoutput plugin, or
// leaves it in place if it already exists. The slot is the server-side bookmark that keeps
// WAL around until we confirm we've processed it.
//
// When it creates the slot it exports a snapshot (EXPORT_SNAPSHOT) so the caller can copy
// existing rows consistently before streaming. The exported snapshot stays valid only
// while this replication connection is idle — i.e. until START_REPLICATION — so the caller
// must run the snapshot before Stream.
func EnsureSlot(ctx context.Context, conn *pgconn.PgConn, slotName string) (SlotInfo, error) {
	res, err := pglogrepl.CreateReplicationSlot(ctx, conn, slotName, outputPlugin,
		pglogrepl.CreateReplicationSlotOptions{Temporary: false, SnapshotAction: "EXPORT_SNAPSHOT"})
	if err == nil {
		lsn, perr := pglogrepl.ParseLSN(res.ConsistentPoint)
		if perr != nil {
			return SlotInfo{}, fmt.Errorf("parse consistent point %q: %w", res.ConsistentPoint, perr)
		}
		return SlotInfo{Created: true, ConsistentPoint: lsn, SnapshotName: res.SnapshotName}, nil
	}

	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == pgErrDuplicateObject {
		// Slot already exists from a previous run — reuse it (no snapshot available).
		return SlotInfo{Created: false}, nil
	}
	return SlotInfo{}, fmt.Errorf("create replication slot %q: %w", slotName, err)
}

// Stream issues START_REPLICATION, which switches the socket into the bidirectional
// CopyBoth state, then loops receiving messages from the server. For now it just logs the
// kind of each message: keepalives ('k') and WAL data ('w'). Decoding the WAL payload and
// sending LSN feedback come in later commits.
//
// startLSN of 0 tells Postgres to resume from the slot's confirmed position.
// saveCheckpoint persists a durably-flushed LSN. It may be nil to disable checkpointing.
type saveCheckpoint func(pglogrepl.LSN) error

func Stream(ctx context.Context, conn *pgconn.PgConn, slot, publication string, startLSN pglogrepl.LSN, snk sink.Sink, save saveCheckpoint) error {
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
	slog.Info("streaming", "slot", slot, "publication", publication, "from", startLSN)

	dec := decode.New()
	// clientXLogPos is the furthest WAL position we've processed; it's what we report back.
	clientXLogPos := startLSN
	nextStandbyDeadline := time.Now().Add(standbyTimeout)

	for {
		// Send periodic feedback so the server can free WAL up to clientXLogPos.
		// Flush the sink first so we only confirm an LSN whose events are durable.
		if time.Now().After(nextStandbyDeadline) {
			if err := flushAndReport(ctx, conn, snk, save, clientXLogPos); err != nil {
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
				slog.Info("shutting down, flushing final position", "lsn", clientXLogPos)
				if ferr := flushAndReport(context.Background(), conn, snk, save, clientXLogPos); ferr != nil {
					slog.Error("final flush/feedback failed", "err", ferr)
				}
				return nil
			}
			return fmt.Errorf("receive message: %w", err)
		}

		cd, ok := msg.(*pgproto3.CopyData)
		if !ok {
			slog.Warn("unexpected message", "type", fmt.Sprintf("%T", msg))
			continue
		}

		switch cd.Data[0] {
		case pglogrepl.PrimaryKeepaliveMessageByteID:
			pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(cd.Data[1:])
			if err != nil {
				return fmt.Errorf("parse keepalive: %w", err)
			}
			metrics.ReplicationLagBytes.Set(float64(pkm.ServerWALEnd - clientXLogPos))
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
					metrics.SinkErrorsTotal.Inc()
					return fmt.Errorf("sink write: %w", err)
				}
				metrics.EventsTotal.WithLabelValues(string(ev.Op)).Inc()
			}
			// Advance past the bytes we just consumed.
			clientXLogPos = xld.WALStart + pglogrepl.LSN(len(xld.WALData))
			metrics.ReplicationLagBytes.Set(float64(xld.ServerWALEnd - clientXLogPos))

		default:
			slog.Warn("unknown CopyData kind", "kind", string(cd.Data[0]))
		}
	}
}

// flushAndReport makes the sink durable up to pos, then reports pos to the server as
// write/flush/apply. Flushing before reporting is the at-least-once guarantee: we never
// tell Postgres it can recycle WAL for events we haven't persisted.
func flushAndReport(ctx context.Context, conn *pgconn.PgConn, snk sink.Sink, save saveCheckpoint, pos pglogrepl.LSN) error {
	if err := snk.Flush(); err != nil {
		metrics.SinkErrorsTotal.Inc()
		return fmt.Errorf("sink flush: %w", err)
	}
	// Persist our own checkpoint before acking the server, so a crash never leaves us
	// resuming earlier than what's already durable in the sink.
	if save != nil {
		if err := save(pos); err != nil {
			return fmt.Errorf("save checkpoint: %w", err)
		}
	}
	err := pglogrepl.SendStandbyStatusUpdate(ctx, conn,
		pglogrepl.StandbyStatusUpdate{WALWritePosition: pos})
	if err != nil {
		return fmt.Errorf("send standby status update: %w", err)
	}
	slog.Debug("flushed sink + checkpoint + reported lsn", "lsn", pos)
	return nil
}
