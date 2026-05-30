// Package checkpoint persists the last durably-processed LSN so the engine can resume from
// exactly where it left off across restarts — independent of the slot's confirmed position.
package checkpoint

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"strings"

	"github.com/jackc/pglogrepl"
)

// Store reads and writes a single LSN to a file.
type Store struct {
	path string
}

// New returns a Store backed by the given file path.
func New(path string) *Store { return &Store{path: path} }

// Load returns the saved LSN. The bool is false when no checkpoint exists yet.
func (s *Store) Load() (pglogrepl.LSN, bool, error) {
	b, err := os.ReadFile(s.path)
	if errors.Is(err, fs.ErrNotExist) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("read checkpoint %q: %w", s.path, err)
	}
	lsn, err := pglogrepl.ParseLSN(strings.TrimSpace(string(b)))
	if err != nil {
		return 0, false, fmt.Errorf("parse checkpoint %q: %w", s.path, err)
	}
	return lsn, true, nil
}

// Save writes the LSN atomically (temp file + rename) so a crash mid-write can't leave a
// partial or corrupt checkpoint.
func (s *Store) Save(lsn pglogrepl.LSN) error {
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, []byte(lsn.String()), 0o644); err != nil {
		return fmt.Errorf("write checkpoint tmp: %w", err)
	}
	if err := os.Rename(tmp, s.path); err != nil {
		return fmt.Errorf("rename checkpoint: %w", err)
	}
	return nil
}
