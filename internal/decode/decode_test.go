package decode

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/rushikeshg25/cdc/internal/event"
)

func TestConvert(t *testing.T) {
	tests := []struct {
		name string
		oid  uint32
		data string
		want any
	}{
		{"bool true", pgtype.BoolOID, "t", true},
		{"bool false", pgtype.BoolOID, "f", false},
		{"int4", pgtype.Int4OID, "42", int64(42)},
		{"int8 negative", pgtype.Int8OID, "-7", int64(-7)},
		{"float8", pgtype.Float8OID, "9.99", 9.99},
		{"jsonb", pgtype.JSONBOID, `{"a":1}`, json.RawMessage(`{"a":1}`)},
		{"text", pgtype.TextOID, "hello", "hello"},
		{"unparseable int falls back to string", pgtype.Int4OID, "notanint", "notanint"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := convert(tt.oid, []byte(tt.data))
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("convert(%d, %q) = %#v, want %#v", tt.oid, tt.data, got, tt.want)
			}
		})
	}
}

// rel is a small users(id int4, name text) relation for tests.
func rel() *pglogrepl.RelationMessage {
	return &pglogrepl.RelationMessage{
		RelationID:   1,
		Namespace:    "public",
		RelationName: "users",
		Columns: []*pglogrepl.RelationMessageColumn{
			{Name: "id", DataType: pgtype.Int4OID},
			{Name: "name", DataType: pgtype.TextOID},
		},
	}
}

func textCol(s string) *pglogrepl.TupleDataColumn {
	return &pglogrepl.TupleDataColumn{DataType: pglogrepl.TupleDataTypeText, Data: []byte(s)}
}

func TestTupleToMap(t *testing.T) {
	tup := &pglogrepl.TupleData{
		ColumnNum: 2,
		Columns: []*pglogrepl.TupleDataColumn{
			textCol("5"),
			{DataType: pglogrepl.TupleDataTypeNull},
		},
	}
	got := tupleToMap(rel(), tup)
	want := map[string]any{"id": int64(5), "name": nil}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("tupleToMap = %#v, want %#v", got, want)
	}
}

func TestTupleToMapOmitsToast(t *testing.T) {
	tup := &pglogrepl.TupleData{
		ColumnNum: 2,
		Columns: []*pglogrepl.TupleDataColumn{
			textCol("5"),
			{DataType: pglogrepl.TupleDataTypeToast}, // unchanged, not sent
		},
	}
	got := tupleToMap(rel(), tup)
	if _, ok := got["name"]; ok {
		t.Errorf("expected unchanged TOAST column to be omitted, got %#v", got)
	}
}

// feedBeginRelation primes the decoder with a transaction and relation so row messages
// can be decoded.
func feedBeginRelation(t *testing.T, d *Decoder) {
	t.Helper()
	begin := &pglogrepl.BeginMessage{Xid: 99, CommitTime: time.Unix(1700000000, 0).UTC()}
	if _, err := d.handle(0, begin); err != nil {
		t.Fatalf("begin: %v", err)
	}
	if _, err := d.handle(0, rel()); err != nil {
		t.Fatalf("relation: %v", err)
	}
}

// one asserts the decoder returned exactly one event and returns it.
func one(t *testing.T, evs []event.ChangeEvent, err error) event.ChangeEvent {
	t.Helper()
	if err != nil {
		t.Fatalf("handle: %v", err)
	}
	if len(evs) != 1 {
		t.Fatalf("expected 1 event, got %d", len(evs))
	}
	return evs[0]
}

func TestHandleInsert(t *testing.T) {
	d := New()
	feedBeginRelation(t, d)

	ins := &pglogrepl.InsertMessage{
		RelationID: 1,
		Tuple:      &pglogrepl.TupleData{ColumnNum: 2, Columns: []*pglogrepl.TupleDataColumn{textCol("1"), textCol("ada")}},
	}
	evs, err := d.handle(pglogrepl.LSN(16), ins)
	ev := one(t, evs, err)
	if ev.Op != event.OpInsert || ev.Table != "users" || ev.Xid != 99 {
		t.Errorf("unexpected event header: %+v", ev)
	}
	if !reflect.DeepEqual(ev.After, map[string]any{"id": int64(1), "name": "ada"}) {
		t.Errorf("after = %#v", ev.After)
	}
	if ev.Before != nil {
		t.Errorf("insert should have no before, got %#v", ev.Before)
	}
}

func TestHandleUpdateAndDelete(t *testing.T) {
	d := New()
	feedBeginRelation(t, d)

	upd := &pglogrepl.UpdateMessage{
		RelationID:   1,
		OldTupleType: pglogrepl.UpdateMessageTupleTypeOld,
		OldTuple:     &pglogrepl.TupleData{ColumnNum: 2, Columns: []*pglogrepl.TupleDataColumn{textCol("1"), textCol("ada")}},
		NewTuple:     &pglogrepl.TupleData{ColumnNum: 2, Columns: []*pglogrepl.TupleDataColumn{textCol("1"), textCol("ada2")}},
	}
	evs, err := d.handle(0, upd)
	ev := one(t, evs, err)
	if ev.Op != event.OpUpdate ||
		ev.Before["name"] != "ada" || ev.After["name"] != "ada2" {
		t.Errorf("unexpected update event: %+v", ev)
	}

	del := &pglogrepl.DeleteMessage{
		RelationID:   1,
		OldTupleType: pglogrepl.DeleteMessageTupleTypeOld,
		OldTuple:     &pglogrepl.TupleData{ColumnNum: 2, Columns: []*pglogrepl.TupleDataColumn{textCol("1"), textCol("ada2")}},
	}
	evs, err = d.handle(0, del)
	ev = one(t, evs, err)
	if ev.Op != event.OpDelete || ev.Before["id"] != int64(1) || ev.After != nil {
		t.Errorf("unexpected delete event: %+v", ev)
	}
}

func TestHandleTruncate(t *testing.T) {
	d := New()
	feedBeginRelation(t, d) // caches relation id 1 (public.users)

	tr := &pglogrepl.TruncateMessage{RelationNum: 2, RelationIDs: []uint32{1, 404}}
	evs, err := d.handle(0, tr)
	if err != nil {
		t.Fatalf("truncate: %v", err)
	}
	// id 404 is unknown and skipped; only the cached relation produces an event.
	if len(evs) != 1 || evs[0].Op != event.OpTruncate || evs[0].Table != "users" {
		t.Errorf("unexpected truncate events: %+v", evs)
	}
}

func TestHandleInsertUnknownRelation(t *testing.T) {
	d := New()
	ins := &pglogrepl.InsertMessage{RelationID: 404, Tuple: &pglogrepl.TupleData{}}
	if _, err := d.handle(0, ins); err == nil {
		t.Error("expected error for unknown relation, got nil")
	}
}
