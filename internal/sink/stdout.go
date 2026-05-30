package sink

import (
	"bufio"
	"encoding/json"
	"os"

	"github.com/rushikeshg25/cdc/internal/event"
)

// Stdout writes each event as a JSON line to standard output. Useful for development and
// piping into other tools.
type Stdout struct {
	w *bufio.Writer
}

// NewStdout returns a Sink that writes JSONL to stdout.
func NewStdout() *Stdout {
	return &Stdout{w: bufio.NewWriter(os.Stdout)}
}

func (s *Stdout) Write(ev event.ChangeEvent) error {
	b, err := json.Marshal(ev)
	if err != nil {
		return err
	}
	if _, err := s.w.Write(b); err != nil {
		return err
	}
	return s.w.WriteByte('\n')
}

func (s *Stdout) Flush() error { return s.w.Flush() }

func (s *Stdout) Close() error { return s.w.Flush() }
