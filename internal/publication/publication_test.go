package publication

import "testing"

func TestQuoteTables(t *testing.T) {
	got, err := quoteTables([]string{"public.users", "items", "myschema.orders"})
	if err != nil {
		t.Fatalf("quoteTables: %v", err)
	}
	want := `"public"."users", "public"."items", "myschema"."orders"`
	if got != want {
		t.Errorf("quoteTables = %s, want %s", got, want)
	}
}

func TestQuoteTablesInvalid(t *testing.T) {
	for _, bad := range []string{"", ".", "schema.", ".table"} {
		if _, err := quoteTables([]string{bad}); err == nil {
			t.Errorf("expected error for %q", bad)
		}
	}
}
