//go:build wasm

package sqlutil

import "context"

// IsTransient always reports false for the in-browser SQLite build, which has
// no connection to lose.
func IsTransient(error) bool { return false }

// IsDatabaseUnavailable always reports false for the in-browser SQLite build.
func IsDatabaseUnavailable(error) bool { return false }

// RetryTransient runs fn once; nothing is transient in the browser build.
func RetryTransient(_ context.Context, _ string, fn func() error) error { return fn() }
