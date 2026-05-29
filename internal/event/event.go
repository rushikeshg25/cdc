// Package event defines the change event produced by decoding and consumed by sinks.
package event

import "time"

// Op is the kind of row change.
type Op string

const (
	OpInsert   Op = "insert"
	OpUpdate   Op = "update"
	OpDelete   Op = "delete"
	OpTruncate Op = "truncate"
)

// ChangeEvent is one decoded row-level change.
type ChangeEvent struct {
	Op     Op     `json:"op"`
	Schema string `json:"schema"`
	Table  string `json:"table"`

	// Before holds the prior row image (UPDATE/DELETE, subject to REPLICA IDENTITY).
	// After holds the new row image (INSERT/UPDATE).
	Before map[string]any `json:"before,omitempty"`
	After  map[string]any `json:"after,omitempty"`

	// LSN is the WAL position of this change.
	LSN string `json:"lsn"`
	// Xid is the transaction id this change belongs to.
	Xid uint32 `json:"xid,omitempty"`
	// CommitTime is the transaction's commit timestamp.
	CommitTime time.Time `json:"commit_time"`
}
