package internal

import (
	"testing"
	"testing/synctest"
	"time"

	"codefloe.com/pat-s/zendrite/setup/process"
)

func TestIntrospectionCacheCleanup(t *testing.T) {
	cache := getIntrospectionCache()
	synctest.Test(t, func(t *testing.T) {
		p := process.NewProcessContextWithParent(t.Context())
		defer p.ShutdownZendrite()
		const key = "lifecycle-expiry-test"
		cache.Set(key, &cachedIntrospection{}, time.Second)
		defer cache.Delete(key)
		before := cache.Len()
		StartIntrospectionCacheCleanup(p.Context())
		time.Sleep(introspectionCacheTTL + time.Second)
		synctest.Wait()
		if cache.Len() >= before {
			t.Fatal("expired cache entry was not evicted")
		}
		p.ShutdownZendrite()
		p.WaitForComponentsToFinish()
	})
}
