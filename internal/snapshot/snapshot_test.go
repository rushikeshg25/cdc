package snapshot

import (
	"reflect"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestRowToMap(t *testing.T) {
	names := []string{"id", "name", "active"}
	values := []any{int64(1), "ada", true}

	got := rowToMap(names, values)
	want := map[string]any{"id": int64(1), "name": "ada", "active": true}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("rowToMap = %#v, want %#v", got, want)
	}
}

func TestRowToMapShortValues(t *testing.T) {
	// Defensive: fewer values than names shouldn't panic.
	got := rowToMap([]string{"a", "b"}, []any{1})
	if len(got) != 1 || got["a"] != 1 {
		t.Errorf("rowToMap short = %#v", got)
	}
}

func TestFieldNames(t *testing.T) {
	fds := []pgconn.FieldDescription{{Name: "id"}, {Name: "name"}}
	got := fieldNames(fds)
	want := []string{"id", "name"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("fieldNames = %#v, want %#v", got, want)
	}
}

func TestQuoteLiteral(t *testing.T) {
	tests := map[string]string{
		"abc":      "'abc'",
		"a'b":      "'a''b'",
		"00007-1A": "'00007-1A'",
	}
	for in, want := range tests {
		if got := quoteLiteral(in); got != want {
			t.Errorf("quoteLiteral(%q) = %q, want %q", in, got, want)
		}
	}
}
