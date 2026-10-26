package pushrules

import "sync"

// userLocks serializes read-modify-write operations on a user's push rules.
// Push rules are stored as one account data document, so concurrent updates
// to different rules must not overwrite one another.
var userLocks = struct {
	sync.Mutex
	locks map[string]*sync.Mutex
}{locks: make(map[string]*sync.Mutex)}

// LockForUser locks push rule updates for userID and returns the unlock
// function.
func LockForUser(userID string) func() {
	userLocks.Lock()
	mu := userLocks.locks[userID]
	if mu == nil {
		mu = &sync.Mutex{}
		userLocks.locks[userID] = mu
	}
	userLocks.Unlock()

	mu.Lock()
	return mu.Unlock
}
