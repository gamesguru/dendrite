package deltas

import (
	"context"
	"database/sql"
)

// UpNotificationThreadCounts stores the per-thread breakdown of unread
// notification counts as JSON. NULL means no breakdown is known.
func UpNotificationThreadCounts(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, `ALTER TABLE syncapi_notification_data ADD COLUMN thread_counts TEXT;`)
	return err
}
