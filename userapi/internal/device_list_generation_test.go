package internal

import (
	"fmt"
	"sync"
	"testing"
)

// Updates for different users hold different per-user mutexes, so the shared
// generation map needs its own lock. Without it this fails under -race and can
// die with "concurrent map writes".
func TestUpdateGenerationIsSafeAcrossUsers(t *testing.T) {
	u := &DeviceListUpdater{
		mu:            &sync.Mutex{},
		userIDToMutex: make(map[string]*sync.Mutex),
	}
	const users, bumps = 16, 200
	var wg sync.WaitGroup
	for i := 0; i < users; i++ {
		userID := fmt.Sprintf("@u%d:remote", i)
		wg.Add(1)
		go func() {
			defer wg.Done()
			mu := u.mutex(userID)
			for j := 0; j < bumps; j++ {
				mu.Lock()
				u.bumpUpdateGenerationLocked(userID)
				mu.Unlock()
				_ = u.currentUpdateGeneration(userID)
			}
		}()
	}
	wg.Wait()
	for i := 0; i < users; i++ {
		userID := fmt.Sprintf("@u%d:remote", i)
		if got := u.currentUpdateGeneration(userID); got != bumps {
			t.Fatalf("%s: generation %d, want %d", userID, got, bumps)
		}
	}
}
