// Package metrics exposes Prometheus metrics for the CDC engine and serves them over HTTP.
package metrics

import (
	"log/slog"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	// EventsTotal counts change events emitted, labeled by op (insert/update/delete/...).
	EventsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "cdc_events_total",
		Help: "Number of change events processed, by op.",
	}, []string{"op"})

	// SinkErrorsTotal counts sink write/flush failures.
	SinkErrorsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "cdc_sink_errors_total",
		Help: "Number of sink write/flush errors.",
	})

	// ReplicationLagBytes is how far behind the server's WAL end we are (bytes).
	ReplicationLagBytes = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "cdc_replication_lag_bytes",
		Help: "Server WAL end minus the client's processed position, in bytes.",
	})
)

// Serve starts a background HTTP server exposing /metrics. An empty addr disables it.
func Serve(addr string) {
	if addr == "" {
		return
	}
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	go func() {
		slog.Info("metrics server listening", "addr", addr)
		if err := http.ListenAndServe(addr, mux); err != nil {
			slog.Error("metrics server stopped", "err", err)
		}
	}()
}
