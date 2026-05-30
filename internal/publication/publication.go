// Package publication manages a Postgres publication scoped to specific tables, which is
// how we filter which tables the CDC engine captures (the natural Postgres mechanism).
package publication

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
)

// Ensure makes the named publication cover exactly the given tables (schema.table). It
// creates the publication FOR TABLE … if absent, or ALTERs an existing one to the same set.
// With no tables it is a no-op (the existing publication is used as-is).
//
// dsn must be a normal (non-replication) connection string; publication DDL is plain SQL.
func Ensure(ctx context.Context, dsn, name string, tables []string) error {
	if len(tables) == 0 {
		return nil
	}

	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return fmt.Errorf("publication connect: %w", err)
	}
	defer conn.Close(ctx)

	tableList, err := quoteTables(tables)
	if err != nil {
		return err
	}
	pub := pgx.Identifier{name}.Sanitize()

	var exists bool
	if err := conn.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = $1)`, name).Scan(&exists); err != nil {
		return fmt.Errorf("check publication: %w", err)
	}

	var sql string
	if exists {
		sql = fmt.Sprintf("ALTER PUBLICATION %s SET TABLE %s", pub, tableList)
	} else {
		sql = fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s", pub, tableList)
	}
	if _, err := conn.Exec(ctx, sql); err != nil {
		return fmt.Errorf("ensure publication (%s): %w", sql, err)
	}
	return nil
}

// quoteTables turns ["public.users", "items"] into a safely-quoted comma list. A bare name
// defaults to the public schema.
func quoteTables(tables []string) (string, error) {
	parts := make([]string, 0, len(tables))
	for _, t := range tables {
		schema, table := "public", t
		if i := strings.IndexByte(t, '.'); i >= 0 {
			schema, table = t[:i], t[i+1:]
		}
		if schema == "" || table == "" {
			return "", fmt.Errorf("invalid table name %q", t)
		}
		parts = append(parts, pgx.Identifier{schema, table}.Sanitize())
	}
	return strings.Join(parts, ", "), nil
}
