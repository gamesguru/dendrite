// Copyright 2026 Patrick Schratz
//
// SPDX-License-Identifier: AGPL-3.0-only.

package threepid

import (
	"crypto/sha256"
	"sync"
	"time"
)

// Validation sessions are short-lived, like interactive authentication sessions.
// Hash the credentials so the cache does not retain client secrets.
var validationSessions = struct {
	sync.Mutex
	entries map[[32]byte]validationSession
}{entries: make(map[[32]byte]validationSession)}

type validationSession struct {
	server  string
	expires time.Time
}

func sessionKey(sid, secret string) [32]byte {
	return sha256.Sum256([]byte(sid + "\x00" + secret))
}

func rememberSession(sid, secret, server string) {
	validationSessions.Lock()
	defer validationSessions.Unlock()
	now := time.Now()
	for key, session := range validationSessions.entries {
		if now.After(session.expires) {
			delete(validationSessions.entries, key)
		}
	}
	validationSessions.entries[sessionKey(sid, secret)] = validationSession{server, now.Add(time.Hour)}
}

// SessionServer resolves credentials issued through the homeserver's token endpoint.
func SessionServer(sid, secret string) string {
	validationSessions.Lock()
	defer validationSessions.Unlock()
	session := validationSessions.entries[sessionKey(sid, secret)]
	if time.Now().After(session.expires) {
		return ""
	}
	return session.server
}
