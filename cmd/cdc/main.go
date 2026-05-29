// Command cdc streams row-level changes out of Postgres via logical replication and
// writes them as JSONL.
package main

import (
	"fmt"
	"os"

	"github.com/rushikeshg25/cdc/internal/config"
)

func main() {
	cfg, err := config.Parse(os.Args[1:])
	if err != nil {
		// flag already printed usage/error for ErrHelp and parse failures.
		os.Exit(2)
	}

	fmt.Printf("cdc config: dsn=%s slot=%s publication=%s output=%s verbose=%t\n",
		cfg.DSN, cfg.SlotName, cfg.Publication, cfg.OutputPath, cfg.Verbose)
}
