package jetstream

import (
	"context"
	"fmt"
	"testing"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"
)

func TestRetryAPI(t *testing.T) {
	t.Run("retries until the cluster answers", func(t *testing.T) {
		attempts := 0
		err := retryAPI(t.Context(), "step", func() error {
			attempts++
			switch attempts {
			case 1:
				return nats.ErrTimeout
			case 2:
				return fmt.Errorf("configure stream: %w", context.DeadlineExceeded)
			case 3:
				return nats.ErrNoResponders
			case 4:
				// A forming cluster that does not know all peers yet.
				return fmt.Errorf("configure stream: %w", &nats.APIError{Code: 400, ErrorCode: errCodeNoSuitablePeers, Description: "no suitable peers for placement"})
			}
			return nil
		})
		require.NoError(t, err)
		require.Equal(t, 5, attempts)
	})
	t.Run("returns other errors at once", func(t *testing.T) {
		refused := &nats.APIError{Code: 400, ErrorCode: nats.JSErrCodeStreamNameInUse, Description: "stream name already in use"}
		attempts := 0
		err := retryAPI(t.Context(), "step", func() error {
			attempts++
			return refused
		})
		require.ErrorIs(t, err, refused)
		require.Equal(t, 1, attempts)
	})
	t.Run("stops when the context ends", func(t *testing.T) {
		ctx, cancel := context.WithCancelCause(t.Context())
		attempts := 0
		err := retryAPI(ctx, "step", func() error {
			attempts++
			cancel(nil)
			return nats.ErrTimeout
		})
		require.ErrorIs(t, err, nats.ErrTimeout)
		require.Equal(t, 1, attempts)
	})
}
