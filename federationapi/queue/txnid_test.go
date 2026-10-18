package queue

import (
	"encoding/json"
	"testing"

	"codefloe.com/pat-s/gomatrixserverlib"
	"github.com/stretchr/testify/require"

	"codefloe.com/pat-s/zendrite/federationapi/statistics"
	"codefloe.com/pat-s/zendrite/test"
)

// TestRetryKeepsTransactionIDOnlyForSameContent checks that a retry reuses a
// failed transaction's ID only when it carries the same content. A
// destination may answer a known ID with its earlier outcome, which would drop
// whatever the retry added.
func TestRetryKeepsTransactionIDOnlyForSameContent(t *testing.T) {
	db, _, closeDB := mustCreateFederationDatabase(t, test.DBTypeSQLite, false)
	t.Cleanup(closeDB)
	stats := statistics.NewStatistics(db, 1, 1, false)
	oq := &destinationQueue{statistics: stats.ForServer("remote.test")}
	txn := func(pdus ...string) gomatrixserverlib.Transaction {
		t := gomatrixserverlib.Transaction{PDUs: []json.RawMessage{}, EDUs: []gomatrixserverlib.EDU{}}
		for _, p := range pdus {
			t.PDUs = append(t.PDUs, json.RawMessage(`{"body":"`+p+`"}`))
		}
		return t
	}
	first := oq.transactionIDFor(txn("a"))
	require.Equal(t, first, oq.transactionIDFor(txn("a")), "a retry of the same content keeps its ID")
	grown := oq.transactionIDFor(txn("a", "b"))
	require.NotEqual(t, first, grown, "a retry carrying more events needs a new ID")
	require.Equal(t, grown, oq.transactionIDFor(txn("a", "b")))
	oq.transactionID = ""
	require.NotEqual(t, grown, oq.transactionIDFor(txn("a", "b")), "a successful send ends the ID")
}
