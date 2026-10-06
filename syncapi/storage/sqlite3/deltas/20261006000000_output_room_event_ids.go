package deltas

import (
	"context"
	"database/sql"
)

// UpRestoreOutputRoomEventIDs fills in the _event_id header that redacting a
// room v3+ event used to store empty. The header is always the last key, and
// the event_id column always held the right ID.
func UpRestoreOutputRoomEventIDs(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, `UPDATE syncapi_output_room_events
		SET headered_event_json = substr(headered_event_json, 1, length(headered_event_json) - 3) || json_quote(event_id) || '}'
		WHERE headered_event_json LIKE '%"\_event\_id":""}' ESCAPE '\'`)
	return err
}
