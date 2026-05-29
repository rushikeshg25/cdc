// Package decode turns pgoutput logical-replication messages into ChangeEvents.
//
// pgoutput streams a sequence of messages per transaction:
//
//	Begin → [Relation] → Insert/Update/Delete... → Commit
//
// Tuple messages (Insert/Update/Delete) reference a table by numeric relation ID and carry
// only column values, not names. A Relation message (sent once per table per session, or
// when the schema changes) describes the columns. We cache relations by ID so we can map
// values back to column names when a change arrives.
package decode

import (
	"fmt"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/rushikeshg25/cdc/internal/event"
)

// Decoder holds the per-session decoding state: the relation cache and the current
// transaction's metadata (set on Begin, used to stamp each change).
type Decoder struct {
	relations map[uint32]*pglogrepl.RelationMessage

	// Current transaction context, from the Begin message.
	xid        uint32
	commitTime time.Time
}

// New returns a ready-to-use Decoder.
func New() *Decoder {
	return &Decoder{relations: make(map[uint32]*pglogrepl.RelationMessage)}
}

// Process decodes a single XLogData payload. It returns a ChangeEvent for row changes
// (insert/update/delete), or nil for framing messages (begin/relation/commit) that only
// update internal state. lsn is the WAL position of this payload.
func (d *Decoder) Process(lsn pglogrepl.LSN, walData []byte) (*event.ChangeEvent, error) {
	msg, err := pglogrepl.Parse(walData)
	if err != nil {
		return nil, fmt.Errorf("parse logical message: %w", err)
	}

	switch m := msg.(type) {
	case *pglogrepl.BeginMessage:
		// Begin carries the transaction id and its commit timestamp up front.
		d.xid = m.Xid
		d.commitTime = m.CommitTime
		return nil, nil

	case *pglogrepl.RelationMessage:
		d.relations[m.RelationID] = m
		return nil, nil

	case *pglogrepl.CommitMessage:
		d.xid = 0
		d.commitTime = time.Time{}
		return nil, nil

	default:
		// Insert/Update/Delete/Truncate handled in later commits.
		return nil, nil
	}
}
