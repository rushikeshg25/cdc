// Package config holds the runtime configuration for the cdc engine and parses it
// from command-line flags.
package config

import "flag"

// Config is everything the cdc engine needs to run.
type Config struct {
	// DSN is the base Postgres connection string. The replication package adds the
	// replication=database parameter required to open a replication connection.
	DSN string
	// SlotName is the logical replication slot to create/reuse.
	SlotName string
	// Publication is the publication whose tables we stream (created by setup).
	Publication string
	// OutputPath is the JSONL file change events are appended to.
	OutputPath string
	// Verbose enables debug-level logging.
	Verbose bool
}

// Parse builds a Config from the given argument list (typically os.Args[1:]).
func Parse(args []string) (Config, error) {
	fs := flag.NewFlagSet("cdc", flag.ContinueOnError)

	var c Config
	fs.StringVar(&c.DSN, "dsn", "postgres://cdc:cdc@localhost:5433/cdc_demo?sslmode=disable", "Postgres connection string")
	fs.StringVar(&c.SlotName, "slot", "cdc_slot", "logical replication slot name")
	fs.StringVar(&c.Publication, "publication", "cdc_pub", "publication to stream")
	fs.StringVar(&c.OutputPath, "output", "events.jsonl", "JSONL output file for change events")
	fs.BoolVar(&c.Verbose, "verbose", false, "enable debug logging")

	if err := fs.Parse(args); err != nil {
		return Config{}, err
	}
	return c, nil
}
