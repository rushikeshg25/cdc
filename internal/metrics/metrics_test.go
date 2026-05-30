package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestCountersIncrement(t *testing.T) {
	before := testutil.ToFloat64(EventsTotal.WithLabelValues("insert"))
	EventsTotal.WithLabelValues("insert").Inc()
	if got := testutil.ToFloat64(EventsTotal.WithLabelValues("insert")); got != before+1 {
		t.Errorf("EventsTotal insert = %v, want %v", got, before+1)
	}

	beforeErr := testutil.ToFloat64(SinkErrorsTotal)
	SinkErrorsTotal.Inc()
	if got := testutil.ToFloat64(SinkErrorsTotal); got != beforeErr+1 {
		t.Errorf("SinkErrorsTotal = %v, want %v", got, beforeErr+1)
	}
}

func TestLagGauge(t *testing.T) {
	ReplicationLagBytes.Set(42)
	if got := testutil.ToFloat64(ReplicationLagBytes); got != 42 {
		t.Errorf("ReplicationLagBytes = %v, want 42", got)
	}
}
