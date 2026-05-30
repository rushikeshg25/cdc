// Command cdc streams row-level changes out of Postgres via logical replication and
// writes them as JSONL.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/rushikeshg25/cdc/internal/config"
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

	if err := run(cfg); err != nil {
		fmt.Fprintln(os.Stderr, "cdc:", err)
		os.Exit(1)
	}
}

func run(cfg config.Config) error {
	// Cancel the context on Ctrl-C / SIGTERM so the stream can shut down cleanly.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	conn, err := replication.Connect(ctx, cfg.DSN)
	if err != nil {
		return err
	}
	// Use a fresh context for close: ctx may already be canceled by a signal.
	defer conn.Close(context.Background())

	fmt.Printf("connected in replication mode (server pid %d)\n", conn.PID())

	sys, err := replication.IdentifySystem(ctx, conn)
	if err != nil {
		return err
	}
	fmt.Printf("system: id=%s timeline=%d db=%s currentWAL=%s\n",
		sys.SystemID, sys.Timeline, sys.DBName, sys.XLogPos)

	slotInfo, err := replication.EnsureSlot(ctx, conn, cfg.SlotName)
	if err != nil {
		return err
	}
	if slotInfo.Created {
		fmt.Printf("slot %q created (consistentPoint=%s snapshot=%s)\n",
			cfg.SlotName, slotInfo.ConsistentPoint, slotInfo.SnapshotName)
	} else {
		fmt.Printf("slot %q already exists, reusing\n", cfg.SlotName)
	}

	snk, err := sink.NewFile(cfg.OutputPath)
	if err != nil {
		return err
	}
	defer func() {
		if cerr := snk.Close(); cerr != nil {
			fmt.Fprintln(os.Stderr, "cdc: sink close:", cerr)
		}
	}()
	fmt.Printf("writing events to %s\n", cfg.OutputPath)

	// On a freshly created slot, snapshot existing rows (consistently, via the exported
	// snapshot) before streaming. Then stream from the slot's consistent point so live
	// changes pick up exactly where the snapshot ended. Snapshot must run before Stream:
	// START_REPLICATION invalidates the exported snapshot.
	if slotInfo.Created {
		n, err := snapshot.Run(ctx, cfg.DSN, slotInfo.SnapshotName, cfg.Publication,
			slotInfo.ConsistentPoint.String(), snk)
		if err != nil {
			return err
		}
		fmt.Printf("snapshot complete: %d rows\n", n)
	}

	// ConsistentPoint is the snapshot boundary when created, or zero (resume from the
	// slot's confirmed position) when reusing an existing slot.
	return replication.Stream(ctx, conn, cfg.SlotName, cfg.Publication, slotInfo.ConsistentPoint, snk)
}
