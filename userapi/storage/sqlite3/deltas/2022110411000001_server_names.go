package deltas

import (
	"context"
	"database/sql"
	"fmt"

	"codefloe.com/pat-s/dendrite/internal/sqlutil"
	"github.com/matrix-org/gomatrixserverlib/spec"
)

// I know what you're thinking: you're wondering "why doesn't this use $1
// and pass variadic parameters to ExecContext?" — the answer is because
// PostgreSQL doesn't expect the table name to be specified as a substituted
// argument in that way so it results in a syntax error in the query.

func UpServerNamesPopulate(ctx context.Context, tx *sql.Tx, serverName spec.ServerName) error {
	for _, table := range serverNamesTables {
		q := fmt.Sprintf(
			"UPDATE %s SET server_name = %s WHERE server_name = '';",
			sqlutil.QuoteIdentifier(table), sqlutil.QuoteLiteral(string(serverName)),
		)
		if _, err := tx.ExecContext(ctx, q); err != nil {
			return fmt.Errorf("write server names to %q error: %w", table, err)
		}
	}
	return nil
}
