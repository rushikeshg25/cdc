// Package config holds the runtime configuration for the cdc engine and parses it
// from command-line flags.
package config

import (
	"flag"
	"strings"
)

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
	// Tables, when non-empty, scopes capture to these schema.table names (via a publication
	// FOR TABLE …). Empty means use the existing publication as-is.
	Tables []string

	// Sink selects the output: file (default), stdout, http, or kafka.
	Sink string
	// HTTPURL is the endpoint for the http sink.
	HTTPURL string
	// KafkaBrokers / KafkaTopic configure the kafka sink.
	KafkaBrokers []string
	KafkaTopic   string

	// MetricsAddr, when non-empty, serves Prometheus metrics at /metrics on that address.
	MetricsAddr string

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
	var tables string
	fs.StringVar(&tables, "tables", "", "comma-separated schema.table list to capture (default: whole publication)")
	fs.StringVar(&c.Sink, "sink", "file", "output sink: file, stdout, http, kafka")
	fs.StringVar(&c.HTTPURL, "http-url", "", "endpoint for the http sink")
	var brokers string
	fs.StringVar(&brokers, "kafka-brokers", "localhost:9092", "comma-separated kafka brokers")
	fs.StringVar(&c.KafkaTopic, "kafka-topic", "", "kafka topic (default: cdc.<schema>.<table>)")
	fs.StringVar(&c.MetricsAddr, "metrics-addr", "", "serve Prometheus /metrics on this address (e.g. :9100)")
	fs.BoolVar(&c.Verbose, "verbose", false, "enable debug logging")

	if err := fs.Parse(args); err != nil {
		return Config{}, err
	}
	c.Tables = splitCSV(tables)
	c.KafkaBrokers = splitCSV(brokers)
	return c, nil
}

// splitCSV parses a comma-separated list, trimming whitespace and dropping empties.
func splitCSV(s string) []string {
	var out []string
	for part := range strings.SplitSeq(s, ",") {
		if t := strings.TrimSpace(part); t != "" {
			out = append(out, t)
		}
	}
	return out
}
