package checkpoint

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/jackc/pglogrepl"
)

func TestLoadMissing(t *testing.T) {
	s := New(filepath.Join(t.TempDir(), "nope.offset"))
	_, ok, err := s.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if ok {
		t.Error("expected ok=false for missing checkpoint")
	}
}

func TestSaveLoadRoundTrip(t *testing.T) {
	s := New(filepath.Join(t.TempDir(), "cdc.offset"))
	want := pglogrepl.LSN(0x19B1280)

	if err := s.Save(want); err != nil {
		t.Fatalf("Save: %v", err)
	}
	got, ok, err := s.Load()
	if err != nil || !ok {
		t.Fatalf("Load: ok=%v err=%v", ok, err)
	}
	if got != want {
		t.Errorf("Load = %s, want %s", got, want)
	}
}

func TestSaveOverwrites(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cdc.offset")
	s := New(path)

	if err := s.Save(pglogrepl.LSN(1)); err != nil {
		t.Fatal(err)
	}
	if err := s.Save(pglogrepl.LSN(2)); err != nil {
		t.Fatal(err)
	}
	got, _, _ := s.Load()
	if got != pglogrepl.LSN(2) {
		t.Errorf("Load = %s, want 0/2", got)
	}
	// No leftover temp file.
	if _, err := os.Stat(path + ".tmp"); !os.IsNotExist(err) {
		t.Errorf("temp file should not remain: %v", err)
	}
}
