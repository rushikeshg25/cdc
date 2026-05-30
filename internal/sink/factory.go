package sink

import "fmt"

// Options selects and configures a sink implementation.
type Options struct {
	Kind     string // "file" (default), "stdout", "http", "kafka"
	FilePath string // for "file"

	HTTPURL string // for "http"

	KafkaBrokers []string // for "kafka"
	KafkaTopic   string   // for "kafka"
}

// New constructs the sink named by o.Kind.
func New(o Options) (Sink, error) {
	switch o.Kind {
	case "", "file":
		return NewFile(o.FilePath)
	case "stdout":
		return NewStdout(), nil
	case "http":
		return NewHTTP(o.HTTPURL)
	default:
		return nil, fmt.Errorf("unknown sink %q", o.Kind)
	}
}
