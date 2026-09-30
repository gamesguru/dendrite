package ha

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"codefloe.com/pat-s/zendrite/setup/config"
)

type stubOwner struct{ err error }

func (o *stubOwner) Generation() int64 { return 1 }
func (o *stubOwner) Identity() string  { return "candidate-a" }
func (o *stubOwner) Valid() error      { return o.err }

func TestAdmitClosesApplicationRoutesOnOwnershipLoss(t *testing.T) {
	owner := &stubOwner{}
	admit := Admit(func() bool { return true }, owner)
	require.True(t, admit())

	owner.err = errors.New("ownership generation is retired")
	require.False(t, admit(), "application routes must close as soon as ownership is lost")
}

func TestAdmitRespectsActivationReadiness(t *testing.T) {
	admit := Admit(func() bool { return false }, &stubOwner{})
	require.False(t, admit(), "an unready activation must not admit requests even while it owns the homeserver")
}

func TestAdmitLeavesStandaloneUnchanged(t *testing.T) {
	ready := true
	admit := Admit(func() bool { return ready }, nil)
	require.True(t, admit())
	ready = false
	require.False(t, admit())
}

type handingOver struct {
	started  chan struct{}
	finished chan struct{}
}

func (h *handingOver) HandOver(time.Duration, time.Duration) {
	close(h.started)
	<-h.finished
}

// Stopping an HA owner cancels ownership only after the application has
// handed over, so its requests in flight are not cut.
func TestPlannedHandoverDelaysOwnershipCancellation(t *testing.T) {
	cfg := config.HA{Enabled: true, HandoverGrace: time.Second, HandoverTimeout: time.Minute}
	app := &handingOver{started: make(chan struct{}), finished: make(chan struct{})}
	stopping, stop := context.WithCancelCause(t.Context())
	ownership := PlannedHandover(stopping, cfg, app)
	stop(nil)
	<-app.started
	require.NoError(t, ownership.Err(), "ownership must outlast the handover")
	close(app.finished)
	<-ownership.Done()
}

func TestPlannedHandoverDisabled(t *testing.T) {
	stopping := t.Context()
	require.Equal(t, stopping, PlannedHandover(stopping, config.HA{Enabled: true}, nil))
	require.Equal(t, stopping, PlannedHandover(stopping, config.HA{HandoverTimeout: time.Minute}, nil))
}
