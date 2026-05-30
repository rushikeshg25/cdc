// Command cdc streams row-level changes out of Postgres via logical replication and
// writes them as JSONL.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/rushikeshg25/cdc/internal/checkpoint"
	"github.com/rushikeshg25/cdc/internal/config"
	"github.com/rushikeshg25/cdc/internal/metrics"
	"github.com/rushikeshg25/cdc/internal/publication"
	"github.com/rushikeshg25/cdc/internal/replication"
	"github.com/rushikeshg25/cdc/internal/sink"
	"github.com/rushikeshg25/cdc/internal/snapshot"
)

func main() {
	cfg, err := config.Parse(os.Args[1:])
	if err != nil {
		// flag already printed usage/error for ErrHelp and parse failures.
		os.Exit(2)
	}

	level := slog.LevelInfo
	if cfg.Verbose {
		level = slog.LevelDebug
	}
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level})))

	if err := run(cfg); err != nil {
		slog.Error("cdc exited", "err", err)
		os.Exit(1)
	}
}

func run(cfg config.Config) error {
	// Cancel the context on Ctrl-C / SIGTERM so the stream can shut down cleanly.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	metrics.Serve(cfg.MetricsAddr)

	conn, err := replication.Connect(ctx, cfg.DSN)
	if err != nil {
		return err
	}
	// Use a fresh context for close: ctx may already be canceled by a signal.
	defer conn.Close(context.Background())

	slog.Info("connected in replication mode", "server_pid", conn.PID())

	sys, err := replication.IdentifySystem(ctx, conn)
	if err != nil {
		return err
	}
	slog.Info("system identified",
		"system_id", sys.SystemID, "timeline", sys.Timeline, "db", sys.DBName, "current_wal", sys.XLogPos)

	// Scope the publication to --tables (if given) before creating the slot/streaming.
	if err := publication.Ensure(ctx, cfg.DSN, cfg.Publication, cfg.Tables); err != nil {
		return err
	}
	if len(cfg.Tables) > 0 {
		slog.Info("publication scoped", "publication", cfg.Publication, "tables", cfg.Tables)
	}

	slotInfo, err := replication.EnsureSlot(ctx, conn, cfg.SlotName)
	if err != nil {
		return err
	}
	if slotInfo.Created {
		slog.Info("slot created", "slot", cfg.SlotName,
			"consistent_point", slotInfo.ConsistentPoint, "snapshot", slotInfo.SnapshotName)
	} else {
		slog.Info("slot reused", "slot", cfg.SlotName)
	}

	snk, err := sink.New(sink.Options{
		Kind:         cfg.Sink,
		FilePath:     cfg.OutputPath,
		HTTPURL:      cfg.HTTPURL,
		KafkaBrokers: cfg.KafkaBrokers,
		KafkaTopic:   cfg.KafkaTopic,
	})
	if err != nil {
		return err
	}
	defer func() {
		if cerr := snk.Close(); cerr != nil {
			slog.Error("sink close", "err", cerr)
		}
	}()
	slog.Info("sink ready", "sink", cfg.Sink)

	cp := checkpoint.New(cfg.OutputPath + ".offset")
	cpLSN, hasCP, err := cp.Load()
	if err != nil {
		return err
	}

	// Decide where to start, and whether to snapshot:
	//   - checkpoint present  -> resume exactly from it; skip snapshot (already done).
	//   - fresh slot, no ckpt -> snapshot existing rows, then stream from consistent point.
	//   - existing slot, no ckpt -> resume from the slot's confirmed position (LSN 0).
	// Snapshot must run before Stream: START_REPLICATION invalidates the exported snapshot.
	startLSN := slotInfo.ConsistentPoint
	switch {
	case hasCP:
		startLSN = cpLSN
		slog.Info("resuming from checkpoint", "lsn", cpLSN, "snapshot", "skipped")
	case slotInfo.Created:
		n, err := snapshot.Run(ctx, cfg.DSN, slotInfo.SnapshotName, cfg.Publication,
			slotInfo.ConsistentPoint.String(), snk)
		if err != nil {
			return err
		}
		slog.Info("snapshot complete", "rows", n)
	default:
		slog.Info("no checkpoint; resuming from slot's confirmed position")
	}

	return replication.Stream(ctx, conn, cfg.SlotName, cfg.Publication, startLSN, snk, cp.Save)
}
