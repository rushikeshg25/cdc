package sink

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/rushikeshg25/cdc/internal/event"
)

// HTTP buffers events and POSTs them as a JSON array on Flush. Because the buffer is only
// cleared after a successful POST, a failing endpoint keeps the events (and the replication
// loop won't advance the LSN), preserving at-least-once delivery.
type HTTP struct {
	url    string
	client *http.Client
	buf    []event.ChangeEvent
}

// NewHTTP returns an HTTP sink posting to url.
func NewHTTP(url string) (*HTTP, error) {
	if url == "" {
		return nil, fmt.Errorf("http sink requires --http-url")
	}
	return &HTTP{url: url, client: &http.Client{Timeout: 30 * time.Second}}, nil
}

func (h *HTTP) Write(ev event.ChangeEvent) error {
	h.buf = append(h.buf, ev)
	return nil
}

func (h *HTTP) Flush() error {
	if len(h.buf) == 0 {
		return nil
	}
	body, err := json.Marshal(h.buf)
	if err != nil {
		return err
	}
	resp, err := h.client.Post(h.url, "application/json", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("http sink post: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("http sink: unexpected status %d", resp.StatusCode)
	}
	h.buf = h.buf[:0] // only clear after a successful delivery
	return nil
}

func (h *HTTP) Close() error { return h.Flush() }
