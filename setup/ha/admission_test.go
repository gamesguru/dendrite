package ha

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
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
