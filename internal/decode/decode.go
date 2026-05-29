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
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgtype"
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

	case *pglogrepl.InsertMessage:
		rel, ok := d.relations[m.RelationID]
		if !ok {
			return nil, fmt.Errorf("insert references unknown relation %d", m.RelationID)
		}
		ev := d.newEvent(event.OpInsert, rel, lsn)
		ev.After = tupleToMap(rel, m.Tuple)
		return &ev, nil

	case *pglogrepl.UpdateMessage:
		rel, ok := d.relations[m.RelationID]
		if !ok {
			return nil, fmt.Errorf("update references unknown relation %d", m.RelationID)
		}
		ev := d.newEvent(event.OpUpdate, rel, lsn)
		// OldTuple is only present per the table's REPLICA IDENTITY (key, or full row).
		if m.OldTuple != nil {
			ev.Before = tupleToMap(rel, m.OldTuple)
		}
		ev.After = tupleToMap(rel, m.NewTuple)
		return &ev, nil

	case *pglogrepl.DeleteMessage:
		rel, ok := d.relations[m.RelationID]
		if !ok {
			return nil, fmt.Errorf("delete references unknown relation %d", m.RelationID)
		}
		ev := d.newEvent(event.OpDelete, rel, lsn)
		// OldTuple holds the deleted row's key (or full row under REPLICA IDENTITY FULL).
		if m.OldTuple != nil {
			ev.Before = tupleToMap(rel, m.OldTuple)
		}
		return &ev, nil

	default:
		// Update/Delete/Truncate handled in later commits.
		return nil, nil
	}
}

// newEvent builds a ChangeEvent stamped with the current relation and transaction context.
func (d *Decoder) newEvent(op event.Op, rel *pglogrepl.RelationMessage, lsn pglogrepl.LSN) event.ChangeEvent {
	return event.ChangeEvent{
		Op:         op,
		Schema:     rel.Namespace,
		Table:      rel.RelationName,
		LSN:        lsn.String(),
		Xid:        d.xid,
		CommitTime: d.commitTime,
	}
}

// tupleToMap resolves a tuple's positional column values against the relation's column
// names. Values are kept as strings here (pgoutput sends text format); typed conversion is
// added in a later commit. Unchanged TOASTed columns ('u') are omitted since their value
// isn't sent; NULLs map to nil.
func tupleToMap(rel *pglogrepl.RelationMessage, tup *pglogrepl.TupleData) map[string]any {
	if tup == nil {
		return nil
	}
	out := make(map[string]any, len(tup.Columns))
	for i, col := range tup.Columns {
		name := rel.Columns[i].Name
		switch col.DataType {
		case pglogrepl.TupleDataTypeNull:
			out[name] = nil
		case pglogrepl.TupleDataTypeToast:
			// Value unchanged and not transmitted; leave it out.
			continue
		default: // text (and binary, if ever)
			out[name] = convert(rel.Columns[i].DataType, col.Data)
		}
	}
	return out
}

// convert turns a column's text-format bytes into a typed Go value based on its type OID,
// so the JSON output has real numbers/booleans/objects instead of strings everywhere.
// Unknown or unparseable types fall back to the raw string.
func convert(oid uint32, data []byte) any {
	s := string(data)
	switch oid {
	case pgtype.BoolOID:
		return s == "t"
	case pgtype.Int2OID, pgtype.Int4OID, pgtype.Int8OID:
		if n, err := strconv.ParseInt(s, 10, 64); err == nil {
			return n
		}
	case pgtype.Float4OID, pgtype.Float8OID:
		if f, err := strconv.ParseFloat(s, 64); err == nil {
			return f
		}
	case pgtype.JSONOID, pgtype.JSONBOID:
		return json.RawMessage(data)
	}
	return s
}
