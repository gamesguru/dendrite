// Copyright 2024 New Vector Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package transactions

import (
	"net/http"
	"net/url"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"

	"github.com/matrix-org/util"

	userapi "codefloe.com/pat-s/zendrite/userapi/api"
)

type fakeType struct {
	ID string `json:"ID"`
}

func TestCompare(t *testing.T) {
	u1, _ := url.Parse("/send/1?accessToken=123")
	u2, _ := url.Parse("/send/1")
	c1 := CacheKey{"1", "2", filepath.Dir(u1.Path)}
	c2 := CacheKey{"1", "2", filepath.Dir(u2.Path)}
	if !reflect.DeepEqual(c1, c2) {
		t.Fatalf("Cache keys differ: %+v <> %+v", c1, c2)
	}
}

var (
	fakeDevice   = &userapi.Device{UserID: "@alice:test", ID: "ALICEDEVICE", AccessToken: "aRandomAccessToken"}
	fakeDevice2  = &userapi.Device{UserID: "@alice:test", ID: "OTHERDEVICE", AccessToken: "anotherRandomAccessToken"}
	fakeTxnID    = "aRandomTxnID"
	fakeResponse = &util.JSONResponse{
		Code: http.StatusOK, JSON: fakeType{ID: "0"},
	}
	fakeResponse2 = &util.JSONResponse{
		Code: http.StatusOK, JSON: fakeType{ID: "1"},
	}
	fakeResponse3 = &util.JSONResponse{
		Code: http.StatusOK, JSON: fakeType{ID: "2"},
	}
)

// TestCache creates a New Cache and tests AddTransaction & FetchTransaction.
func TestCache(t *testing.T) {
	fakeTxnCache := New()
	u, _ := url.Parse("")
	fakeTxnCache.AddTransaction(fakeDevice, fakeTxnID, u, fakeResponse)

	// Add entries for noise.
	for i := 1; i <= 100; i++ {
		fakeTxnCache.AddTransaction(
			fakeDevice,
			fakeTxnID+strconv.Itoa(i),
			u,
			&util.JSONResponse{Code: http.StatusOK, JSON: fakeType{ID: strconv.Itoa(i)}},
		)
	}

	testResponse, ok := fakeTxnCache.FetchTransaction(fakeDevice, fakeTxnID, u)
	if !ok {
		t.Error("Failed to retrieve entry for txnID: ", fakeTxnID)
	} else if testResponse.JSON != fakeResponse.JSON {
		t.Error("Fetched response incorrect. Expected: ", fakeResponse.JSON, " got: ", testResponse.JSON)
	}
}

// TestCacheScope ensures transactions with the same transaction ID are not shared
// across multiple devices and endpoints.
func TestCacheScope(t *testing.T) {
	cache := New()
	sendEndpoint, _ := url.Parse("/send/1?accessToken=test")
	sendToDeviceEndpoint, _ := url.Parse("/sendToDevice/1")
	cache.AddTransaction(fakeDevice, fakeTxnID, sendEndpoint, fakeResponse)
	cache.AddTransaction(fakeDevice2, fakeTxnID, sendEndpoint, fakeResponse2)
	cache.AddTransaction(fakeDevice2, fakeTxnID, sendToDeviceEndpoint, fakeResponse3)

	if res, ok := cache.FetchTransaction(fakeDevice, fakeTxnID, sendEndpoint); !ok {
		t.Errorf("failed to retrieve entry for (%s, %s)", fakeDevice.ID, fakeTxnID)
	} else if res.JSON != fakeResponse.JSON {
		t.Errorf("Wrong cache entry for (%s, %s). Expected: %v; got: %v", fakeDevice.ID, fakeTxnID, fakeResponse.JSON, res.JSON)
	}
	if res, ok := cache.FetchTransaction(fakeDevice2, fakeTxnID, sendEndpoint); !ok {
		t.Errorf("failed to retrieve entry for (%s, %s)", fakeDevice.ID, fakeTxnID)
	} else if res.JSON != fakeResponse2.JSON {
		t.Errorf("Wrong cache entry for (%s, %s). Expected: %v; got: %v", fakeDevice.ID, fakeTxnID, fakeResponse2.JSON, res.JSON)
	}

	// Ensure the txnID is not shared across endpoints
	if res, ok := cache.FetchTransaction(fakeDevice2, fakeTxnID, sendToDeviceEndpoint); !ok {
		t.Errorf("failed to retrieve entry for (%s, %s)", fakeDevice.ID, fakeTxnID)
	} else if res.JSON != fakeResponse3.JSON {
		t.Errorf("Wrong cache entry for (%s, %s). Expected: %v; got: %v", fakeDevice.ID, fakeTxnID, fakeResponse2.JSON, res.JSON)
	}
}

// TestCacheScopeDevice ensures transactions are scoped to a device rather than an
// access token (MSC3970), and that application services share one scope.
func TestCacheScopeDevice(t *testing.T) {
	cache := New()
	sendEndpoint, _ := url.Parse("/send/1")

	cache.AddTransaction(fakeDevice, fakeTxnID, sendEndpoint, fakeResponse)
	refreshed := *fakeDevice
	refreshed.AccessToken = "aRefreshedAccessToken"
	refreshed.SessionID = 2
	if res, ok := cache.FetchTransaction(&refreshed, fakeTxnID, sendEndpoint); !ok || res.JSON != fakeResponse.JSON {
		t.Errorf("transaction not found after refreshing the access token of the device")
	}
	otherUser := *fakeDevice
	otherUser.UserID = "@bob:test"
	if _, ok := cache.FetchTransaction(&otherUser, fakeTxnID, sendEndpoint); ok {
		t.Errorf("transaction shared with another user's device of the same ID")
	}

	asAlice := &userapi.Device{UserID: "@alice_bridge:test", ID: "AS_Device", AccessToken: "asToken", AppserviceID: "bridge"}
	asBob := &userapi.Device{UserID: "@bob_bridge:test", ID: "AS_Device", AccessToken: "asToken", AppserviceID: "bridge"}
	otherAS := &userapi.Device{UserID: "@alice_bridge:test", ID: "AS_Device", AccessToken: "otherToken", AppserviceID: "other"}
	cache.AddTransaction(asAlice, fakeTxnID, sendEndpoint, fakeResponse2)
	if res, ok := cache.FetchTransaction(asBob, fakeTxnID, sendEndpoint); !ok || res.JSON != fakeResponse2.JSON {
		t.Errorf("transaction not shared between users of the same application service")
	}
	if _, ok := cache.FetchTransaction(otherAS, fakeTxnID, sendEndpoint); ok {
		t.Errorf("transaction shared with another application service")
	}
}
