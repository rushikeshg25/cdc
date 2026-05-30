// Command cdc streams row-level changes out of Postgres via logical replication and
// writes them as JSONL.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/rushikeshg25/cdc/internal/checkpoint"
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
		fmt.Printf("resuming from checkpoint %s (skipping snapshot)\n", cpLSN)
	case slotInfo.Created:
		n, err := snapshot.Run(ctx, cfg.DSN, slotInfo.SnapshotName, cfg.Publication,
			slotInfo.ConsistentPoint.String(), snk)
		if err != nil {
			return err
		}
		fmt.Printf("snapshot complete: %d rows\n", n)
	default:
		fmt.Println("no checkpoint; resuming from slot's confirmed position")
	}

	return replication.Stream(ctx, conn, cfg.SlotName, cfg.Publication, startLSN, snk, cp.Save)
}
