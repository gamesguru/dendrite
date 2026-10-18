// Package ownership reports whether this process still owns the homeserver.
//
// HA binds every protected effect to one authorized activation. Components
// consult an Owner before performing an effect that a successor must not
// overlap with. Standalone deployments have no Owner and are unaffected.
package ownership

import "errors"

// ErrDispatchRefused marks a refusal that came from this homeserver losing
// ownership rather than from the remote party.
//
// Callers that record per-destination failure state must not charge an
// ownership refusal to a remote server: the destination told us nothing, and
// the resulting retry, relay and blacklist state outlives the process in a
// database the successor inherits.
var ErrDispatchRefused = errors.New("dispatch refused: ownership is no longer held")

// Owner reports the authorized activation. Valid must fail closed: it reports
// an error before promotion, after retirement, and whenever ownership has not
// been confirmed recently enough to be trusted.
type Owner interface {
	Generation() int64
	Identity() string
	Valid() error
}
