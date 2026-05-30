package config

import (
	"reflect"
	"testing"
)

func TestParseDefaults(t *testing.T) {
	c, err := Parse(nil)
	if err != nil {
		t.Fatalf("Parse(nil) error: %v", err)
	}
	want := Config{
		DSN:          "postgres://cdc:cdc@localhost:5433/cdc_demo?sslmode=disable",
		SlotName:     "cdc_slot",
		Publication:  "cdc_pub",
		OutputPath:   "events.jsonl",
		Sink:         "file",
		KafkaBrokers: []string{"localhost:9092"},
		Verbose:      false,
	}
	if !reflect.DeepEqual(c, want) {
		t.Errorf("defaults =\n  %+v\nwant\n  %+v", c, want)
	}
}

func TestParseOverrides(t *testing.T) {
	args := []string{
		"--dsn", "postgres://x/y",
		"--slot", "s1",
		"--publication", "p1",
		"--output", "out.jsonl",
		"--verbose",
	}
	c, err := Parse(args)
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}
	want := Config{
		DSN:          "postgres://x/y",
		SlotName:     "s1",
		Publication:  "p1",
		OutputPath:   "out.jsonl",
		Sink:         "file",
		KafkaBrokers: []string{"localhost:9092"},
		Verbose:      true,
	}
	if !reflect.DeepEqual(c, want) {
		t.Errorf("overrides =\n  %+v\nwant\n  %+v", c, want)
	}
}

func TestParseTables(t *testing.T) {
	c, err := Parse([]string{"--tables", " public.users, public.items ,, "})
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	want := []string{"public.users", "public.items"}
	if !reflect.DeepEqual(c.Tables, want) {
		t.Errorf("Tables = %#v, want %#v", c.Tables, want)
	}

	c, _ = Parse(nil)
	if c.Tables != nil {
		t.Errorf("default Tables = %#v, want nil", c.Tables)
	}
}

func TestParseUnknownFlag(t *testing.T) {
	if _, err := Parse([]string{"--nope"}); err == nil {
		t.Error("expected error for unknown flag, got nil")
	}
}
