// Copyright 2024 New Vector Ltd.
// Copyright 2017 Vector Creations Ltd
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package routing

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"sync"
	"time"

	"codefloe.com/pat-s/gomatrixserverlib"
	"codefloe.com/pat-s/gomatrixserverlib/fclient"
	"codefloe.com/pat-s/gomatrixserverlib/spec"
	"github.com/matrix-org/util"

	"codefloe.com/pat-s/zendrite/federationapi/producers"
	"codefloe.com/pat-s/zendrite/internal"
	"codefloe.com/pat-s/zendrite/roomserver/api"
	"codefloe.com/pat-s/zendrite/setup/config"
	userAPI "codefloe.com/pat-s/zendrite/userapi/api"
)

const (
	// Event was passed to the Roomserver.
	MetricsOutcomeOK = "ok"
	// Event failed to be processed.
	MetricsOutcomeFail = "fail"
	// Event failed auth checks.
	MetricsOutcomeRejected = "rejected"
	// Terminated the transaction.
	MetricsOutcomeFatal = "fatal"
	// The event has missing auth_events we need to fetch.
	MetricsWorkMissingAuthEvents = "missing_auth_events"
	// No work had to be done as we had all prev/auth events.
	MetricsWorkDirect = "direct"
	// The event has missing prev_events we need to call /g_m_e for.
	MetricsWorkMissingPrevEvents = "missing_prev_events"
)

var inFlightTxnsPerOrigin sync.Map // origin and transaction ID -> *inFlightTxn

// inFlightTxn is a transaction being processed. Every request carrying the
// same transaction ID while it runs receives its result once done is closed.
type inFlightTxn struct {
	done chan struct{}
	res  util.JSONResponse
}

// transactionTimeout bounds processing a transaction once it was accepted.
//
// Processing does not stop when the sender disconnects: a sender that times
// out retries the transaction, often with the same ID, and an attempt
// abandoned halfway would leave some of its EDUs queued and others dropped
// while the retry joining it was told the outcome of the abandoned one. The
// bound matches the broker's deduplication window, so a publication retried
// throughout it is still stored once. Var for tests.
var transactionTimeout = 2 * time.Minute

// txnEvents is the content of an inbound transaction.
type txnEvents struct {
	PDUs []json.RawMessage       `json:"pdus"`
	EDUs []gomatrixserverlib.EDU `json:"edus"`
}

// Send implements /_matrix/federation/v1/send/{txnID}.
func Send(
	httpReq *http.Request,
	request *fclient.FederationRequest,
	txnID gomatrixserverlib.TransactionID,
	cfg *config.FederationAPI,
	rsAPI api.FederationRoomserverAPI,
	keyAPI userAPI.FederationUserAPI,
	keys gomatrixserverlib.JSONVerifier,
	federation fclient.FederationClient,
	mu *internal.MutexByRoom,
	producer *producers.SyncAPIProducer,
) util.JSONResponse {
	var txn txnEvents
	if err := json.Unmarshal(request.Content(), &txn); err != nil {
		return util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.NotJSON("The request body could not be decoded into valid JSON. " + err.Error()),
		}
	}
	// Transactions are limited in size; they can have at most 50 PDUs and 100 EDUs.
	// https://matrix.org/docs/spec/server_server/latest#transactions
	if len(txn.PDUs) > 50 || len(txn.EDUs) > 100 {
		return util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.BadJSON("max 50 pdus / 100 edus"),
		}
	}

	// First we should check if this origin has already submitted this
	// transaction to us and it is still being worked on. If so, the new
	// request waits for it to complete rather than creating more work.
	//
	// A retry joins only an attempt with the same content. Some senders,
	// older Zendrite and Dendrite among them, reuse a failed transaction's ID
	// for a retry that carries more events; answering that with the earlier
	// attempt's outcome would drop everything that was added.
	index := string(request.Origin()) + "\000" + string(txnID) + "\000" + txn.digest()
	v, loaded := inFlightTxnsPerOrigin.LoadOrStore(index, &inFlightTxn{done: make(chan struct{})})
	inFlight, ok := v.(*inFlightTxn)
	if !ok {
		panic("unexpected type in inFlightTxnsPerOrigin")
	}
	if loaded {
		// This origin already submitted this txn ID to us, and the work
		// is still taking place, so we'll just wait for it to finish.
		ctx, cancel := context.WithTimeout(httpReq.Context(), time.Minute*5) //nolint:mnd
		defer cancel()
		select {
		case <-ctx.Done():
			// If the caller gives up then return straight away. We don't
			// want to attempt to process what they sent us any further.
			return util.JSONResponse{Code: http.StatusRequestTimeout}
		case <-inFlight.done:
			// The original task just finished processing so let's return
			// the result of it.
			return inFlight.res
		}
	}
	// Otherwise, we are working on this txn from this origin. Requests
	// joining it receive its result; if processing panics, they are told to
	// retry.
	inFlight.res = util.JSONResponse{Code: http.StatusInternalServerError, JSON: spec.InternalServerError{}}
	defer close(inFlight.done)
	defer inFlightTxnsPerOrigin.Delete(index)
	inFlight.res = processTransaction(httpReq, request, txnID, txn, cfg, rsAPI, keyAPI, keys, mu, producer)
	return inFlight.res
}

// digest identifies a transaction's content, ignoring fields such as
// origin_server_ts that a sender sets afresh for every attempt.
func (txn txnEvents) digest() string {
	content, err := json.Marshal(txn)
	if err != nil {
		return ""
	}
	sum := sha256.Sum256(content)
	return hex.EncodeToString(sum[:])
}

func processTransaction(
	httpReq *http.Request,
	request *fclient.FederationRequest,
	txnID gomatrixserverlib.TransactionID,
	txnEvents txnEvents,
	cfg *config.FederationAPI,
	rsAPI api.FederationRoomserverAPI,
	keyAPI userAPI.FederationUserAPI,
	keys gomatrixserverlib.JSONVerifier,
	mu *internal.MutexByRoom,
	producer *producers.SyncAPIProducer,
) util.JSONResponse {
	t := internal.NewTxnReq(
		rsAPI,
		keyAPI,
		cfg.Matrix.ServerName,
		keys,
		mu,
		producer,
		cfg.Matrix.Presence.EnableInbound,
		txnEvents.PDUs,
		txnEvents.EDUs,
		request.Origin(),
		txnID,
		cfg.Matrix.ServerName)

	util.GetLogger(httpReq.Context()).Debugf("Received transaction %q from %q containing %d PDUs, %d EDUs", txnID, request.Origin(), len(t.PDUs), len(t.EDUs))

	ctx, cancel := context.WithTimeout(context.WithoutCancel(httpReq.Context()), transactionTimeout)
	defer cancel()
	resp, jsonErr := t.ProcessTransaction(ctx)
	if jsonErr != nil {
		util.GetLogger(httpReq.Context()).WithField("jsonErr", jsonErr).Error("t.processTransaction failed")
		return *jsonErr
	}

	// https://matrix.org/docs/spec/server_server/r0.1.3#put-matrix-federation-v1-send-txnid
	// Status code 200:
	// The result of processing the transaction. The server is to use this response
	// even in the event of one or more PDUs failing to be processed.
	return util.JSONResponse{
		Code: http.StatusOK,
		JSON: resp,
	}
}
