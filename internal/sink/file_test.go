package sink

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/rushikeshg25/cdc/internal/event"
)

func TestFileSinkWritesJSONL(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.jsonl")

	s, err := NewFile(path)
	if err != nil {
		t.Fatalf("NewFile: %v", err)
	}
	want := []event.ChangeEvent{
		{Op: event.OpInsert, Schema: "public", Table: "users", After: map[string]any{"id": float64(1)}},
		{Op: event.OpDelete, Schema: "public", Table: "users", Before: map[string]any{"id": float64(2)}},
	}
	for _, ev := range want {
		if err := s.Write(ev); err != nil {
			t.Fatalf("Write: %v", err)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reopen and decode each line back.
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer f.Close()

	var got []event.ChangeEvent
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		var ev event.ChangeEvent
		if err := json.Unmarshal(sc.Bytes(), &ev); err != nil {
			t.Fatalf("unmarshal line %q: %v", sc.Text(), err)
		}
		got = append(got, ev)
	}
	if err := sc.Err(); err != nil {
		t.Fatalf("scan: %v", err)
	}

	if len(got) != len(want) {
		t.Fatalf("got %d lines, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i].Op != want[i].Op || got[i].Table != want[i].Table {
			t.Errorf("line %d = %+v, want op=%s table=%s", i, got[i], want[i].Op, want[i].Table)
		}
	}
}

func TestFileSinkAppends(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.jsonl")

	for range 2 {
		s, err := NewFile(path)
		if err != nil {
			t.Fatalf("NewFile: %v", err)
		}
		if err := s.Write(event.ChangeEvent{Op: event.OpInsert, Table: "t"}); err != nil {
			t.Fatalf("Write: %v", err)
		}
		if err := s.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	}

	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	// Two separate opens should have appended, not truncated: 2 lines.
	lines := 0
	for _, c := range b {
		if c == '\n' {
			lines++
		}
	}
	if lines != 2 {
		t.Errorf("expected 2 appended lines, got %d", lines)
	}
}
