package ha

import (
	"context"
	"time"

	"codefloe.com/pat-s/zendrite/internal/ownership"
	"codefloe.com/pat-s/zendrite/setup/config"
)

// Admit reports whether the homeserver may accept application requests.
//
// Kubernetes endpoint removal is not the only gate: a request already parked
// on an established keep-alive connection never passes through a new
// readiness check. Admission is therefore evaluated per request, so a
// candidate that has lost ownership rejects traffic immediately rather than
// waiting to be removed from a Service.
//
// A nil owner is standalone, where activation readiness is the only condition.
func Admit(ready func() bool, owner ownership.Owner) func() bool {
	if owner == nil {
		return ready
	}
	return func() bool { return ready() && owner.Valid() == nil }
}

// HandingOver is an application that can stop without failing requests.
type HandingOver interface {
	HandOver(grace, timeout time.Duration)
}

// PlannedHandover returns a context that ends once stopping has ended and the
// application has handed over: told routers to send new requests elsewhere
// and finished the ones in flight, within the configured timeout.
//
// Stopping is the operator's request, a SIGTERM from a rollout or a pod
// deletion. Canceling ownership at once would close admission and cancel
// every request in flight; delaying it lets a router hold new requests for
// the successor instead, so a planned upgrade fails none of them. Ownership
// loss is unaffected, because it cancels the application directly rather
// than through this context.
func PlannedHandover(stopping context.Context, cfg config.HA, app HandingOver) context.Context {
	if !cfg.Enabled || cfg.HandoverTimeout <= 0 {
		return stopping
	}
	ctx, cancel := context.WithCancelCause(context.WithoutCancel(stopping))
	context.AfterFunc(stopping, func() {
		app.HandOver(cfg.HandoverGrace, cfg.HandoverTimeout)
		cancel(context.Cause(stopping))
	})
	return ctx
}
