package helpers

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"codefloe.com/pat-s/zendrite/roomserver/types"
	"codefloe.com/pat-s/zendrite/test"
)

// An outlier is not part of the room DAG, so storing one must not make the
// events it names in prev_events stop being forward extremities. Storing the
// same event later as a normal event records the reference.
func TestStoreOutlierEventDoesNotReferencePrevEvents(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		ctx := context.Background()
		db, closeDB := mustCreateDatabase(t, dbType)
		defer closeDB()

		room := test.NewRoom(t, test.NewUser(t))
		events := room.Events()
		last := events[len(events)-1]
		require.Len(t, last.PrevEventIDs(), 1)
		prevID := last.PrevEventIDs()[0]

		store := func(outlier bool, i int) {
			ev := events[i]
			roomInfo, err := db.GetOrCreateRoomInfo(ctx, ev.PDU)
			require.NoError(t, err)
			typeNID, err := db.GetOrCreateEventTypeNID(ctx, ev.Type())
			require.NoError(t, err)
			stateKeyNID, err := db.GetOrCreateEventStateKeyNID(ctx, ev.StateKey())
			require.NoError(t, err)
			storeEvent := db.StoreEvent
			if outlier {
				storeEvent = db.StoreOutlierEvent
			}
			_, _, err = storeEvent(ctx, ev.PDU, roomInfo, typeNID, stateKeyNID, []types.EventNID{}, false)
			require.NoError(t, err)
		}
		isReferenced := func() bool {
			roomInfo, err := db.RoomInfo(ctx, room.ID)
			require.NoError(t, err)
			updater, err := db.GetRoomUpdater(ctx, roomInfo)
			require.NoError(t, err)
			defer updater.Rollback() //nolint:errcheck
			referenced, err := updater.IsReferenced(prevID)
			require.NoError(t, err)
			return referenced
		}

		for i := range events[:len(events)-1] {
			store(false, i)
		}
		store(true, len(events)-1)
		require.False(t, isReferenced(), "an outlier must not reference its prev_events")

		store(false, len(events)-1)
		require.True(t, isReferenced(), "storing the event normally records the reference")
	})
}
