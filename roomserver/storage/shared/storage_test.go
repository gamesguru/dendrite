package shared_test

import (
	"context"
	"crypto/ed25519"
	"testing"
	"time"

	"github.com/gamesguru/gomatrixserverlib"
	"github.com/gamesguru/gomatrixserverlib/spec"
	"github.com/stretchr/testify/assert"

	"codefloe.com/pat-s/zendrite/internal/caching"
	"codefloe.com/pat-s/zendrite/internal/sqlutil"
	"codefloe.com/pat-s/zendrite/roomserver/storage/postgres"
	"codefloe.com/pat-s/zendrite/roomserver/storage/shared"
	"codefloe.com/pat-s/zendrite/roomserver/storage/sqlite3"
	"codefloe.com/pat-s/zendrite/roomserver/storage/tables"
	"codefloe.com/pat-s/zendrite/roomserver/types"
	"codefloe.com/pat-s/zendrite/setup/config"
	"codefloe.com/pat-s/zendrite/test"
)

func mustCreateRoomserverDatabase(t *testing.T, dbType test.DBType) (*shared.Database, func()) {
	t.Helper()

	connStr, clearDB := test.PrepareDBConnectionString(t, dbType)
	dbOpts := &config.DatabaseOptions{ConnectionString: config.DataSource(connStr)}

	writer := sqlutil.NewExclusiveWriter()
	db, err := sqlutil.Open(dbOpts, writer)
	assert.NoError(t, err)

	var membershipTable tables.Membership
	var stateKeyTable tables.EventStateKeys
	var userRoomKeys tables.UserRoomKeys
	var roomsTable tables.Rooms
	switch dbType {
	case test.DBTypePostgres:
		err = postgres.CreateRoomsTable(db)
		assert.NoError(t, err)
		err = postgres.CreateEventStateKeysTable(db)
		assert.NoError(t, err)
		err = postgres.CreateMembershipTable(db)
		assert.NoError(t, err)
		err = postgres.CreatePartialStateTable(db)
		assert.NoError(t, err)
		err = postgres.CreateUserRoomKeysTable(db)
		assert.NoError(t, err)
		roomsTable, err = postgres.PrepareRoomsTable(db)
		assert.NoError(t, err)
		membershipTable, err = postgres.PrepareMembershipTable(db)
		assert.NoError(t, err)
		stateKeyTable, err = postgres.PrepareEventStateKeysTable(db)
		assert.NoError(t, err)
		userRoomKeys, err = postgres.PrepareUserRoomKeysTable(db)
	case test.DBTypeSQLite:
		err = sqlite3.CreateRoomsTable(db)
		assert.NoError(t, err)
		err = sqlite3.CreateEventStateKeysTable(db)
		assert.NoError(t, err)
		err = sqlite3.CreateMembershipTable(db)
		assert.NoError(t, err)
		err = sqlite3.CreatePartialStateTable(db)
		assert.NoError(t, err)
		err = sqlite3.CreateUserRoomKeysTable(db)
		assert.NoError(t, err)
		roomsTable, err = sqlite3.PrepareRoomsTable(db)
		assert.NoError(t, err)
		membershipTable, err = sqlite3.PrepareMembershipTable(db)
		assert.NoError(t, err)
		stateKeyTable, err = sqlite3.PrepareEventStateKeysTable(db)
		assert.NoError(t, err)
		userRoomKeys, err = sqlite3.PrepareUserRoomKeysTable(db)
	}
	assert.NoError(t, err)

	cache := caching.NewRistrettoCache(8*1024*1024, time.Hour, false)

	evDb := shared.EventDatabase{EventStateKeysTable: stateKeyTable, Cache: cache, Writer: writer}

	database := &shared.Database{
		DB:               db,
		EventDatabase:    evDb,
		MembershipTable:  membershipTable,
		UserRoomKeyTable: userRoomKeys,
		RoomsTable:       roomsTable,
		Writer:           writer,
		Cache:            cache,
	}
	return database, func() {
		clearDB()
		err = db.Close()
		assert.NoError(t, err)
	}
}

func Test_GetLeftUsers(t *testing.T) {
	alice := test.NewUser(t)
	bob := test.NewUser(t)
	charlie := test.NewUser(t)

	ctx := context.Background()
	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		db, close := mustCreateRoomserverDatabase(t, dbType) //nolint:contextcheck
		defer close()

		// Create dummy entries
		for _, user := range []*test.User{alice, bob, charlie} {
			nid, err := db.EventStateKeysTable.InsertEventStateKeyNID(ctx, nil, user.ID)
			assert.NoError(t, err)
			err = db.MembershipTable.InsertMembership(ctx, nil, 1, nid, true)
			assert.NoError(t, err)
			// We must update the membership with a non-zero event NID or it will get filtered out in later queries
			membershipNID := tables.MembershipStateLeaveOrBan
			if user == alice {
				membershipNID = tables.MembershipStateJoin
			}
			_, err = db.MembershipTable.UpdateMembership(ctx, nil, 1, nid, nid, membershipNID, 1, false)
			assert.NoError(t, err)
		}

		// Now try to get the left users, this should be Bob and Charlie, since they have a "leave" membership
		expectedUserIDs := []string{bob.ID, charlie.ID}
		leftUsers, err := db.GetLeftUsers(context.Background(), []string{alice.ID, bob.ID, charlie.ID}) //nolint:contextcheck
		assert.NoError(t, err)
		assert.ElementsMatch(t, expectedUserIDs, leftUsers)
	})
}

func Test_GetUsersSharingRoomWithLocal(t *testing.T) {
	ctx := context.Background()
	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		db, close := mustCreateRoomserverDatabase(t, dbType) //nolint:contextcheck
		defer close()

		join := func(roomNID types.RoomNID, userID string, local bool, eventNID types.EventNID, membership tables.MembershipState) {
			nid, err := db.EventStateKeysTable.InsertEventStateKeyNID(ctx, nil, userID)
			assert.NoError(t, err)
			assert.NoError(t, db.MembershipTable.InsertMembership(ctx, nil, roomNID, nid, local))
			_, err = db.MembershipTable.UpdateMembership(ctx, nil, roomNID, nid, nid, membership, eventNID, false)
			assert.NoError(t, err)
		}
		const joined = tables.MembershipStateJoin

		// Room 1: a local user and remote users, one of whom has left.
		join(1, "@local:localhost", true, 1, joined)
		join(1, "@shared:remote", false, 2, joined)
		join(1, "@left:remote", false, 3, tables.MembershipStateLeaveOrBan)
		// Room 2: remote users only.
		join(2, "@alone:remote", false, 4, joined)
		// Room 3: the local user has left, the remote user is still joined.
		join(3, "@local:localhost", true, 5, tables.MembershipStateLeaveOrBan)
		join(3, "@abandoned:remote", false, 6, joined)
		// Room 4: partial state, our join is event 10. Only members joining after it count.
		assert.NoError(t, db.PartialStateTable.InsertPartialStateRoom(ctx, nil, 4, 10, "remote", nil, 0))
		join(4, "@local:localhost", true, 10, joined)
		join(4, "@preexisting:remote", false, 7, joined)
		join(4, "@newcomer:remote", false, 20, joined)

		users := []string{
			"@local:localhost", "@shared:remote", "@left:remote", "@alone:remote",
			"@abandoned:remote", "@preexisting:remote", "@newcomer:remote", "@unknown:remote",
		}
		got, err := db.GetUsersSharingRoomWithLocal(context.Background(), users) //nolint:contextcheck
		assert.NoError(t, err)
		assert.ElementsMatch(t, []string{"@local:localhost", "@shared:remote", "@newcomer:remote"}, got)
	})
}

func TestUserRoomKeys(t *testing.T) {
	ctx := context.Background()
	alice := test.NewUser(t)
	room := test.NewRoom(t, alice)

	userID, err := spec.NewUserID(alice.ID, true)
	assert.NoError(t, err)
	roomID, err := spec.NewRoomID(room.ID)
	assert.NoError(t, err)

	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		db, close := mustCreateRoomserverDatabase(t, dbType) //nolint:contextcheck
		defer close()

		// create a room NID so we can query the room
		_, err := db.RoomsTable.InsertRoomNID(ctx, nil, roomID.String(), gomatrixserverlib.RoomVersionV10)
		assert.NoError(t, err)
		doesNotExist, err := spec.NewRoomID("!doesnotexist:localhost")
		assert.NoError(t, err)
		_, err = db.RoomsTable.InsertRoomNID(ctx, nil, doesNotExist.String(), gomatrixserverlib.RoomVersionV10)
		assert.NoError(t, err)

		_, key, err := ed25519.GenerateKey(nil)
		assert.NoError(t, err)

		gotKey, err := db.InsertUserRoomPrivatePublicKey(ctx, *userID, *roomID, key)
		assert.NoError(t, err)
		assert.Equal(t, gotKey, key)

		// again, this shouldn't result in an error, but return the existing key
		_, key2, err := ed25519.GenerateKey(nil)
		assert.NoError(t, err)
		gotKey, err = db.InsertUserRoomPrivatePublicKey(ctx, *userID, *roomID, key2)
		assert.NoError(t, err)
		assert.Equal(t, gotKey, key)

		gotKey, err = db.SelectUserRoomPrivateKey(context.Background(), *userID, *roomID) //nolint:contextcheck
		assert.NoError(t, err)
		assert.Equal(t, key, gotKey)
		pubKey, err := db.SelectUserRoomPublicKey(context.Background(), *userID, *roomID) //nolint:contextcheck
		assert.NoError(t, err)
		assert.Equal(t, key.Public(), pubKey)

		// Key doesn't exist, we shouldn't get anything back
		gotKey, err = db.SelectUserRoomPrivateKey(context.Background(), *userID, *doesNotExist) //nolint:contextcheck
		assert.NoError(t, err)
		assert.Nil(t, gotKey)
		pubKey, err = db.SelectUserRoomPublicKey(context.Background(), *userID, *doesNotExist) //nolint:contextcheck
		assert.NoError(t, err)
		assert.Nil(t, pubKey)

		pubKeyForQuery, ok := key.Public().(ed25519.PublicKey)
		assert.True(t, ok, "key.Public() should be ed25519.PublicKey")
		queryUserIDs := map[spec.RoomID][]ed25519.PublicKey{
			*roomID: {pubKeyForQuery},
		}

		userIDs, err := db.SelectUserIDsForPublicKeys(ctx, queryUserIDs)
		assert.NoError(t, err)
		pubKeyForWant, ok := key.Public().(ed25519.PublicKey)
		assert.True(t, ok, "key.Public() should be ed25519.PublicKey")
		wantKeys := map[spec.RoomID]map[string]string{
			*roomID: {
				spec.Base64Bytes(pubKeyForWant).Encode(): userID.String(),
			},
		}
		assert.Equal(t, wantKeys, userIDs)

		// insert key that came in over federation
		var gotPublicKey, key4 ed25519.PublicKey
		key4, _, err = ed25519.GenerateKey(nil)
		assert.NoError(t, err)
		gotPublicKey, err = db.InsertUserRoomPublicKey(context.Background(), *userID, *doesNotExist, key4) //nolint:contextcheck
		assert.NoError(t, err)
		assert.Equal(t, key4, gotPublicKey)

		// test invalid room
		reallyDoesNotExist, err := spec.NewRoomID("!reallydoesnotexist:localhost")
		assert.NoError(t, err)
		_, err = db.InsertUserRoomPublicKey(context.Background(), *userID, *reallyDoesNotExist, key4) //nolint:contextcheck
		assert.Error(t, err)
		_, err = db.InsertUserRoomPrivatePublicKey(context.Background(), *userID, *reallyDoesNotExist, key) //nolint:contextcheck
		assert.Error(t, err)
	})
}

func TestAssignRoomNID(t *testing.T) {
	ctx := context.Background()
	alice := test.NewUser(t)
	room := test.NewRoom(t, alice)

	roomID, err := spec.NewRoomID(room.ID)
	assert.NoError(t, err)

	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		db, close := mustCreateRoomserverDatabase(t, dbType) //nolint:contextcheck
		defer close()

		nid, err := db.AssignRoomNID(ctx, *roomID, room.Version)
		assert.NoError(t, err)
		assert.Greater(t, nid, types.EventNID(0))

		_, err = db.AssignRoomNID(ctx, spec.RoomID{}, "notaroomversion")
		assert.Error(t, err)
	})
}
