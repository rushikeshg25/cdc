// Package sink defines where decoded change events go, and provides implementations.
//
// The Sink contract is what lets the replication loop guarantee at-least-once delivery:
// the loop only reports an LSN back to Postgres after Flush() has durably persisted every
// event written up to that point. A Write may buffer; only Flush must make data durable.
package sink

import "github.com/rushikeshg25/cdc/internal/event"

// Sink consumes change events. Write may buffer; Flush must make all buffered writes
// durable; Close flushes and releases resources.
type Sink interface {
	Write(event.ChangeEvent) error
	Flush() error
	Close() error
}
