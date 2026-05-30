package sink

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/rushikeshg25/cdc/internal/event"
	"github.com/segmentio/kafka-go"
)

// Kafka buffers events and publishes them on Flush. Each message is keyed by schema.table
// (hash-partitioned) so changes to the same table keep their order. The buffer is cleared
// only after a successful write, preserving at-least-once delivery.
type Kafka struct {
	w     *kafka.Writer
	topic string // fixed topic, or "" to route per table
	buf   []kafka.Message
}

// NewKafka returns a Kafka sink. If topic is empty, each event goes to cdc.<schema>.<table>.
func NewKafka(brokers []string, topic string) (*Kafka, error) {
	if len(brokers) == 0 {
		return nil, fmt.Errorf("kafka sink requires --kafka-brokers")
	}
	w := &kafka.Writer{
		Addr:                   kafka.TCP(brokers...),
		Balancer:               &kafka.Hash{}, // partition by key (schema.table)
		AllowAutoTopicCreation: true,
	}
	return &Kafka{w: w, topic: topic}, nil
}

func (k *Kafka) Write(ev event.ChangeEvent) error {
	b, err := json.Marshal(ev)
	if err != nil {
		return err
	}
	k.buf = append(k.buf, kafka.Message{
		Topic: topicFor(k.topic, ev),
		Key:   []byte(ev.Schema + "." + ev.Table),
		Value: b,
	})
	return nil
}

func (k *Kafka) Flush() error {
	if len(k.buf) == 0 {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := k.w.WriteMessages(ctx, k.buf...); err != nil {
		return fmt.Errorf("kafka write: %w", err)
	}
	k.buf = k.buf[:0]
	return nil
}

func (k *Kafka) Close() error {
	if err := k.Flush(); err != nil {
		return err
	}
	return k.w.Close()
}

// topicFor returns the fixed topic if set, else a per-table topic cdc.<schema>.<table>.
func topicFor(fixed string, ev event.ChangeEvent) string {
	if fixed != "" {
		return fixed
	}
	return fmt.Sprintf("cdc.%s.%s", ev.Schema, ev.Table)
}
