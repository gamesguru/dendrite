package ha

import "codefloe.com/pat-s/zendrite/internal/ownership"

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
