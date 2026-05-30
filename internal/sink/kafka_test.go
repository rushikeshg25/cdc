package sink

import (
	"testing"

	"github.com/rushikeshg25/cdc/internal/event"
)

func TestTopicFor(t *testing.T) {
	ev := event.ChangeEvent{Schema: "public", Table: "users"}

	if got := topicFor("", ev); got != "cdc.public.users" {
		t.Errorf("per-table topic = %q, want cdc.public.users", got)
	}
	if got := topicFor("fixed_topic", ev); got != "fixed_topic" {
		t.Errorf("fixed topic = %q, want fixed_topic", got)
	}
}

func TestNewKafkaRequiresBrokers(t *testing.T) {
	if _, err := NewKafka(nil, ""); err == nil {
		t.Error("expected error for no brokers")
	}
}
