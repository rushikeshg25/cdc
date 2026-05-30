// Package snapshot copies existing rows as a consistent baseline before streaming begins.
//
// It uses a *normal* (non-replication) connection that imports the snapshot exported when
// the replication slot was created (SET TRANSACTION SNAPSHOT). Reading existing rows under
// that snapshot, then streaming from the slot's consistent point, yields every row exactly
// once with no gap or duplicate at the boundary.
package snapshot

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// Table is a schema-qualified table name.
type Table struct {
	Schema string
	Name   string
}

// ListPublicationTables returns the tables a publication streams. These are exactly the
// tables we snapshot, so filtering via the publication flows through automatically.
func ListPublicationTables(ctx context.Context, conn *pgx.Conn, publication string) ([]Table, error) {
	rows, err := conn.Query(ctx,
		`SELECT schemaname, tablename
		   FROM pg_publication_tables
		  WHERE pubname = $1
		  ORDER BY schemaname, tablename`, publication)
	if err != nil {
		return nil, fmt.Errorf("query publication tables: %w", err)
	}
	defer rows.Close()

	var tables []Table
	for rows.Next() {
		var t Table
		if err := rows.Scan(&t.Schema, &t.Name); err != nil {
			return nil, fmt.Errorf("scan publication table: %w", err)
		}
		tables = append(tables, t)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate publication tables: %w", err)
	}
	return tables, nil
}
