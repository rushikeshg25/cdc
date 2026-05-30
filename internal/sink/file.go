package sink

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"

	"github.com/rushikeshg25/cdc/internal/event"
)

// File appends events as JSON lines to a file. Writes are buffered; Flush flushes the
// buffer and fsyncs so the replication loop can safely advance the confirmed LSN.
type File struct {
	f *os.File
	w *bufio.Writer
}

// NewFile opens (creating if needed) path for appending JSONL.
func NewFile(path string) (*File, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open sink file %q: %w", path, err)
	}
	return &File{f: f, w: bufio.NewWriter(f)}, nil
}

func (s *File) Write(ev event.ChangeEvent) error {
	b, err := json.Marshal(ev)
	if err != nil {
		return err
	}
	if _, err := s.w.Write(b); err != nil {
		return err
	}
	return s.w.WriteByte('\n')
}

// Flush flushes the buffer to the OS and fsyncs to disk for durability.
func (s *File) Flush() error {
	if err := s.w.Flush(); err != nil {
		return err
	}
	return s.f.Sync()
}

// Close flushes and closes the underlying file.
func (s *File) Close() error {
	if err := s.Flush(); err != nil {
		return err
	}
	return s.f.Close()
}
