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

	created, err := replication.EnsureSlot(ctx, conn, cfg.SlotName)
	if err != nil {
		return err
	}
	if created {
		fmt.Printf("slot %q created\n", cfg.SlotName)
	} else {
		fmt.Printf("slot %q already exists, reusing\n", cfg.SlotName)
	}

	// 0 = resume from the slot's confirmed position.
	return replication.Stream(ctx, conn, cfg.SlotName, cfg.Publication, 0)
}
