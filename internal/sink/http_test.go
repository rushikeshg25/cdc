package sink

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/rushikeshg25/cdc/internal/event"
)

func TestHTTPSinkPostsBatchOnFlush(t *testing.T) {
	var (
		mu       sync.Mutex
		received [][]event.ChangeEvent
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var batch []event.ChangeEvent
		if err := json.Unmarshal(body, &batch); err != nil {
			t.Errorf("server: bad body: %v", err)
		}
		mu.Lock()
		received = append(received, batch)
		mu.Unlock()
	}))
	defer srv.Close()

	s, err := NewHTTP(srv.URL)
	if err != nil {
		t.Fatalf("NewHTTP: %v", err)
	}

	// Buffered until Flush.
	_ = s.Write(event.ChangeEvent{Op: event.OpInsert, Table: "users"})
	_ = s.Write(event.ChangeEvent{Op: event.OpDelete, Table: "users"})

	mu.Lock()
	if len(received) != 0 {
		t.Errorf("expected no POST before flush, got %d", len(received))
	}
	mu.Unlock()

	if err := s.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(received) != 1 || len(received[0]) != 2 {
		t.Fatalf("expected one batch of 2, got %#v", received)
	}
	if received[0][0].Op != event.OpInsert || received[0][1].Op != event.OpDelete {
		t.Errorf("unexpected batch contents: %#v", received[0])
	}
}

func TestHTTPSinkRetainsOnError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	s, _ := NewHTTP(srv.URL)
	_ = s.Write(event.ChangeEvent{Op: event.OpInsert, Table: "t"})

	if err := s.Flush(); err == nil {
		t.Error("expected error from 500 response")
	}
	// Buffer retained so the event isn't lost.
	if len(s.buf) != 1 {
		t.Errorf("expected buffer retained on failure, got %d", len(s.buf))
	}
}

func TestNewHTTPRequiresURL(t *testing.T) {
	if _, err := NewHTTP(""); err == nil {
		t.Error("expected error for empty url")
	}
}
