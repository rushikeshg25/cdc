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
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/rushikeshg25/cdc/internal/event"
	"github.com/rushikeshg25/cdc/internal/sink"
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

// Run copies all rows of the publication's tables as OpRead events, viewed through the
// exported snapshot so the result is a consistent point-in-time image. lsn is the slot's
// consistent point, stamped on each read event. It returns the number of rows emitted.
//
// It opens its own normal connection (dsn must NOT carry replication=database).
func Run(ctx context.Context, dsn, snapshotName, publication, lsn string, snk sink.Sink) (int, error) {
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return 0, fmt.Errorf("snapshot connect: %w", err)
	}
	defer conn.Close(ctx)

	// List tables before opening the snapshot transaction (SET TRANSACTION SNAPSHOT must be
	// the first statement in its transaction).
	tables, err := ListPublicationTables(ctx, conn, publication)
	if err != nil {
		return 0, err
	}

	tx, err := conn.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("snapshot begin: %w", err)
	}
	defer tx.Rollback(ctx)

	// Import the exported snapshot so our SELECTs see exactly the state at the slot's
	// consistent point. Must be the first statement in the transaction.
	if _, err := tx.Exec(ctx, "SET TRANSACTION SNAPSHOT "+quoteLiteral(snapshotName)); err != nil {
		return 0, fmt.Errorf("set transaction snapshot: %w", err)
	}

	total := 0
	for _, t := range tables {
		n, err := copyTable(ctx, tx, t, lsn, snk)
		if err != nil {
			return total, err
		}
		total += n
	}
	return total, tx.Commit(ctx)
}

// copyTable streams one table's rows to the sink as OpRead events.
func copyTable(ctx context.Context, tx pgx.Tx, t Table, lsn string, snk sink.Sink) (int, error) {
	ident := pgx.Identifier{t.Schema, t.Name}.Sanitize()
	rows, err := tx.Query(ctx, "SELECT * FROM "+ident)
	if err != nil {
		return 0, fmt.Errorf("select %s: %w", ident, err)
	}
	defer rows.Close()

	names := fieldNames(rows.FieldDescriptions())
	n := 0
	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return n, fmt.Errorf("read row from %s: %w", ident, err)
		}
		ev := event.ChangeEvent{
			Op:     event.OpRead,
			Schema: t.Schema,
			Table:  t.Name,
			After:  rowToMap(names, values),
			LSN:    lsn,
		}
		if err := snk.Write(ev); err != nil {
			return n, fmt.Errorf("sink write (snapshot %s): %w", ident, err)
		}
		n++
	}
	return n, rows.Err()
}

// fieldNames extracts column names from a result's field descriptions.
func fieldNames(fds []pgconn.FieldDescription) []string {
	names := make([]string, len(fds))
	for i, fd := range fds {
		names[i] = fd.Name
	}
	return names
}

// rowToMap zips column names with their decoded values.
func rowToMap(names []string, values []any) map[string]any {
	m := make(map[string]any, len(names))
	for i, name := range names {
		if i < len(values) {
			m[name] = values[i]
		}
	}
	return m
}

// quoteLiteral wraps s in single quotes, doubling any embedded quotes.
func quoteLiteral(s string) string {
	out := make([]byte, 0, len(s)+2)
	out = append(out, '\'')
	for i := 0; i < len(s); i++ {
		if s[i] == '\'' {
			out = append(out, '\'')
		}
		out = append(out, s[i])
	}
	return string(append(out, '\''))
}
