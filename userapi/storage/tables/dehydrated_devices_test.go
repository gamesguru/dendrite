package tables_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"testing"

	"codefloe.com/pat-s/zendrite/internal/sqlutil"
	"codefloe.com/pat-s/zendrite/setup/config"
	"codefloe.com/pat-s/zendrite/test"
	"codefloe.com/pat-s/zendrite/userapi/storage/postgres"
	"codefloe.com/pat-s/zendrite/userapi/storage/sqlite3"
	"codefloe.com/pat-s/zendrite/userapi/storage/tables"
)

func mustCreateDehydratedDevicesTable(t *testing.T, dbType test.DBType) (tab tables.DehydratedDevicesTable, close func()) {
	t.Helper()
	connStr, close := test.PrepareDBConnectionString(t, dbType)
	db, err := sqlutil.Open(&config.DatabaseOptions{
		ConnectionString: config.DataSource(connStr),
	}, nil)
	if err != nil {
		t.Fatalf("failed to open database: %s", err)
	}
	switch dbType {
	case test.DBTypePostgres:
		tab, err = postgres.NewPostgresDehydratedDevicesTable(db)
	case test.DBTypeSQLite:
		tab, err = sqlite3.NewSQLiteDehydratedDevicesTable(db)
	}
	if err != nil {
		t.Fatalf("failed to create dehydrated devices table: %s", err)
	}
	return tab, close
}

func TestDehydratedDevices(t *testing.T) {
	userID := "@alice:localhost"
	deviceID := "DEHYDRATED_ABC"
	deviceData := json.RawMessage(`{"algorithm":"m.dehydration.v1.olm"}`)
	ctx := context.Background()

	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		tab, closeDB := mustCreateDehydratedDevicesTable(t, dbType)
		defer closeDB()

		// Select from empty table should return ErrNoRows.
		_, _, err := tab.SelectDehydratedDevice(ctx, userID)
		if !errors.Is(err, sql.ErrNoRows) {
			t.Fatalf("expected sql.ErrNoRows, got %v", err)
		}

		// Delete from empty table should return empty device ID.
		deletedID, err := tab.DeleteDehydratedDevice(ctx, nil, userID)
		if err != nil {
			t.Fatalf("unexpected error deleting non-existent device: %s", err)
		}
		if deletedID != "" {
			t.Fatalf("expected empty device ID, got %q", deletedID)
		}

		// Insert a dehydrated device.
		if err = tab.InsertDehydratedDevice(ctx, nil, userID, deviceID, deviceData); err != nil {
			t.Fatalf("failed to insert dehydrated device: %s", err)
		}

		// Select should return the device.
		gotDeviceID, gotDeviceData, err := tab.SelectDehydratedDevice(ctx, userID)
		if err != nil {
			t.Fatalf("failed to select dehydrated device: %s", err)
		}
		if gotDeviceID != deviceID {
			t.Fatalf("expected device ID %q, got %q", deviceID, gotDeviceID)
		}
		if string(gotDeviceData) != string(deviceData) {
			t.Fatalf("expected device data %s, got %s", deviceData, gotDeviceData)
		}

		// Upsert should replace the existing device.
		newDeviceID := "DEHYDRATED_DEF"
		newDeviceData := json.RawMessage(`{"algorithm":"m.dehydration.v2"}`)
		if err = tab.InsertDehydratedDevice(ctx, nil, userID, newDeviceID, newDeviceData); err != nil {
			t.Fatalf("failed to upsert dehydrated device: %s", err)
		}

		gotDeviceID, gotDeviceData, err = tab.SelectDehydratedDevice(ctx, userID)
		if err != nil {
			t.Fatalf("failed to select after upsert: %s", err)
		}
		if gotDeviceID != newDeviceID {
			t.Fatalf("expected device ID %q after upsert, got %q", newDeviceID, gotDeviceID)
		}
		if string(gotDeviceData) != string(newDeviceData) {
			t.Fatalf("expected device data %s after upsert, got %s", newDeviceData, gotDeviceData)
		}

		// Delete should return the device ID.
		deletedID, err = tab.DeleteDehydratedDevice(ctx, nil, userID)
		if err != nil {
			t.Fatalf("failed to delete dehydrated device: %s", err)
		}
		if deletedID != newDeviceID {
			t.Fatalf("expected deleted device ID %q, got %q", newDeviceID, deletedID)
		}

		// Select after delete should return ErrNoRows.
		_, _, err = tab.SelectDehydratedDevice(ctx, userID)
		if !errors.Is(err, sql.ErrNoRows) {
			t.Fatalf("expected sql.ErrNoRows after delete, got %v", err)
		}

		// A second user should be independent.
		otherUserID := "@bob:localhost"
		otherDeviceID := "DEHYDRATED_BOB"
		if err = tab.InsertDehydratedDevice(ctx, nil, userID, deviceID, deviceData); err != nil {
			t.Fatalf("failed to insert alice's device: %s", err)
		}
		if err = tab.InsertDehydratedDevice(ctx, nil, otherUserID, otherDeviceID, deviceData); err != nil {
			t.Fatalf("failed to insert bob's device: %s", err)
		}

		gotDeviceID, _, err = tab.SelectDehydratedDevice(ctx, userID)
		if err != nil {
			t.Fatalf("failed to select alice's device: %s", err)
		}
		if gotDeviceID != deviceID {
			t.Fatalf("expected alice's device %q, got %q", deviceID, gotDeviceID)
		}

		gotDeviceID, _, err = tab.SelectDehydratedDevice(ctx, otherUserID)
		if err != nil {
			t.Fatalf("failed to select bob's device: %s", err)
		}
		if gotDeviceID != otherDeviceID {
			t.Fatalf("expected bob's device %q, got %q", otherDeviceID, gotDeviceID)
		}

		// Deleting alice's device should not affect bob's.
		_, err = tab.DeleteDehydratedDevice(ctx, nil, userID)
		if err != nil {
			t.Fatalf("failed to delete alice's device: %s", err)
		}
		gotDeviceID, _, err = tab.SelectDehydratedDevice(ctx, otherUserID)
		if err != nil {
			t.Fatalf("bob's device should still exist: %s", err)
		}
		if gotDeviceID != otherDeviceID {
			t.Fatalf("expected bob's device %q, got %q", otherDeviceID, gotDeviceID)
		}
	})
}
