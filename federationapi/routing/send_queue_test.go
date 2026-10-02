package routing

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"codefloe.com/pat-s/gomatrixserverlib"
	"codefloe.com/pat-s/gomatrixserverlib/fclient"
	"codefloe.com/pat-s/gomatrixserverlib/spec"
	"github.com/matrix-org/util"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"

	"codefloe.com/pat-s/zendrite/federationapi/producers"
	"codefloe.com/pat-s/zendrite/roomserver/api"
	"codefloe.com/pat-s/zendrite/setup/config"
)

type notBannedRsAPI struct{ api.FederationRoomserverAPI }

func (notBannedRsAPI) QueryServerBannedFromRoom(_ context.Context, _ *api.QueryServerBannedFromRoomRequest, res *api.QueryServerBannedFromRoomResponse) error {
	res.Banned = false
	return nil
}

// slowJetStream answers a publication after a delay, unless the caller's
// context ends first, as a broker electing a leader does.
type slowJetStream struct {
	nats.JetStreamContext
	delay   time.Duration
	err     error
	started chan struct{}
	release chan struct{}

	mu        sync.Mutex
	published int
	failed    []error
}

func (s *slowJetStream) PublishMsg(_ *nats.Msg, opts ...nats.PubOpt) (*nats.PubAck, error) {
	ctx := context.Background()
	for _, o := range opts {
		if c, ok := o.(nats.ContextOpt); ok {
			ctx = c.Context
		}
	}
	s.mu.Lock()
	if s.started != nil {
		close(s.started)
		s.started = nil
	}
	s.mu.Unlock()
	wait := time.After(s.delay)
	if s.release != nil {
		wait = nil
	}
	select {
	case <-ctx.Done():
		s.mu.Lock()
		s.failed = append(s.failed, ctx.Err())
		s.mu.Unlock()
		return nil, ctx.Err()
	case <-wait:
	case <-s.release:
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return nil, s.err
	}
	s.published++
	return &nats.PubAck{}, nil
}

func receiptTxn(t *testing.T, txnID string, events ...string) (*http.Request, *fclient.FederationRequest) {
	t.Helper()
	if len(events) == 0 {
		events = []string{"$e"}
	}
	var edus []gomatrixserverlib.EDU
	for _, event := range events {
		edus = append(edus, gomatrixserverlib.EDU{Type: spec.MReceipt, Content: spec.RawJSON(
			`{"!room:local.test":{"m.read":{"@rita:remote.test":{"data":{"ts":1},"event_ids":["` + event + `"]}}}}`)})
	}
	req := fclient.NewFederationRequest(http.MethodPut, "remote.test", "local.test", "/_matrix/federation/v1/send/"+txnID)
	require.NoError(t, req.SetContent(map[string]any{"pdus": []any{}, "edus": edus}))
	return httptest.NewRequest(http.MethodPut, "/_matrix/federation/v1/send/"+txnID, nil), &req
}

func sendWith(httpReq *http.Request, req *fclient.FederationRequest, txnID string, js nats.JetStreamContext) util.JSONResponse {
	cfg := &config.Zendrite{}
	cfg.Defaults(config.DefaultOpts{Generate: true, SingleDatabase: true})
	cfg.Global.ServerName = "local.test"
	producer := &producers.SyncAPIProducer{JetStream: js, TopicReceiptEvent: "receipts", Config: &cfg.FederationAPI}
	return Send(httpReq, req, gomatrixserverlib.TransactionID(txnID), &cfg.FederationAPI, notBannedRsAPI{}, nil, nil, nil, nil, producer)
}

// TestSendOutlivesTheSender checks that a sender giving up on a transaction
// does not abandon it halfway: the receipt is still queued once the broker
// answers, rather than dropped by the canceled request.
func TestSendOutlivesTheSender(t *testing.T) {
	httpReq, req := receiptTxn(t, "outlives")
	ctx, cancel := context.WithCancelCause(httpReq.Context())
	js := &slowJetStream{delay: 200 * time.Millisecond}
	time.AfterFunc(50*time.Millisecond, func() { cancel(errors.New("the sender gave up")) })
	res := sendWith(httpReq.WithContext(ctx), req, "outlives", js)
	require.Equal(t, http.StatusOK, res.Code)
	require.Equal(t, 1, js.published)
	require.Empty(t, js.failed)
}

// TestSendRetryJoiningGetsTheOutcome checks that a retry arriving while the
// first attempt is still processing receives that attempt's outcome. When the
// receipt could not be queued, both are told to retry; neither may be told
// the transaction was delivered.
func TestSendRetryJoiningGetsTheOutcome(t *testing.T) {
	js := &slowJetStream{err: errors.New("refused"), started: make(chan struct{}), release: make(chan struct{})}
	started := js.started
	results := make(chan int, 2)
	go func() {
		httpReq, req := receiptTxn(t, "joined")
		results <- sendWith(httpReq, req, "joined", js).Code
	}()
	<-started
	go func() {
		httpReq, req := receiptTxn(t, "joined")
		results <- sendWith(httpReq, req, "joined", js).Code
	}()
	// Let the retry join before the first attempt finishes.
	time.Sleep(100 * time.Millisecond)
	close(js.release)
	require.Equal(t, http.StatusServiceUnavailable, <-results)
	require.Equal(t, http.StatusServiceUnavailable, <-results)
}

// TestSendRetryWithMoreContentIsProcessed covers a sender that reuses a
// failed transaction's ID for a retry carrying more items while the first
// attempt is still processing. The retry must be processed itself, not
// answered with the first attempt's outcome.
func TestSendRetryWithMoreContentIsProcessed(t *testing.T) {
	js := &slowJetStream{started: make(chan struct{}), release: make(chan struct{})}
	started := js.started
	results := make(chan int, 2)
	go func() {
		httpReq, req := receiptTxn(t, "grown", "$first")
		results <- sendWith(httpReq, req, "grown", js).Code
	}()
	<-started
	go func() {
		httpReq, req := receiptTxn(t, "grown", "$first", "$added")
		results <- sendWith(httpReq, req, "grown", js).Code
	}()
	time.Sleep(100 * time.Millisecond)
	close(js.release)
	require.Equal(t, http.StatusOK, <-results)
	require.Equal(t, http.StatusOK, <-results)
	js.mu.Lock()
	defer js.mu.Unlock()
	require.Equal(t, 3, js.published, "the retry's receipts were queued, not answered with the first attempt's outcome")
}
