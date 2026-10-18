package jetstream

import (
	"context"
	"errors"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/sirupsen/logrus"
)

// Setup waits up to apiRetryTimeout for a cluster to answer JetStream API
// requests, such as while it elects a meta or stream leader.
const (
	apiRetryTimeout = time.Minute
	apiRetryDelay   = 250 * time.Millisecond
)

// JetStream API errors from a cluster that cannot place or serve assets yet,
// as while brokers join, rejoin or elect a meta leader.
const (
	errCodeNoSuitablePeers        nats.ErrorCode = 10005
	errCodeTemporarilyUnavailable nats.ErrorCode = 10008
	errCodeInsufficientResources  nats.ErrorCode = 10023
)

// retryableAPIError reports JetStream API failures that say nothing about the
// request itself: no answer in time, no server able to answer yet, or a
// cluster that does not yet know enough peers to place the replicas.
func retryableAPIError(err error) bool {
	var jsErr nats.JetStreamError
	if errors.As(err, &jsErr) && jsErr.APIError() != nil {
		switch jsErr.APIError().ErrorCode {
		case errCodeNoSuitablePeers, errCodeTemporarilyUnavailable, errCodeInsufficientResources:
			return true
		}
	}
	return errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, nats.ErrTimeout) ||
		errors.Is(err, nats.ErrJetStreamNotEnabled) ||
		IsTransientFetchError(err)
}

// retryAPI repeats an idempotent JetStream setup step while the cluster is
// too busy to answer, for up to apiRetryTimeout. A fresh or degraded cluster
// can take longer than one request's timeout to place a replicated stream or
// consumer, and failing setup then would turn a leader election into an outage.
// Other errors, and cancellation of ctx, end the retries at once.
func retryAPI(ctx context.Context, what string, step func() error) error {
	ctx, cancel := context.WithTimeout(ctx, apiRetryTimeout)
	defer cancel()
	for {
		err := step()
		if err == nil || !retryableAPIError(err) || ctx.Err() != nil {
			return err
		}
		logrus.WithError(err).Debugf("Retrying JetStream setup: %s", what)
		select {
		case <-ctx.Done():
			return err
		case <-time.After(apiRetryDelay):
		}
	}
}
