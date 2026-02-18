// Copyright 2024 New Vector Ltd.
// Copyright 2020 The Matrix.org Foundation C.I.C.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

// Package hooks exposes places in Zendrite where custom code can be executed, useful for MSCs.
// Hooks can only be run in monolith mode.
package hooks

import (
	"sync"
)

const (
	// KindNewEventPersisted is a hook which is called with *types.HeaderedEvent
	// It is run when a new event is persisted in the roomserver.
	// Usage:
	//   hooks.Attach(hooks.KindNewEventPersisted, func(headeredEvent interface{}) { ... })
	KindNewEventPersisted = "new_event_persisted"
)

var (
	hookMap = make(map[string][]func(any))
	hookMu  = sync.Mutex{}
	enabled = false
)

// Enable all hooks. This may slow down the server slightly. Required for MSCs to work.
func Enable() {
	enabled = true
}

// Run any hooks.
func Run(kind string, data any) {
	if !enabled {
		return
	}
	cbs := callbacks(kind)
	for _, cb := range cbs {
		cb(data)
	}
}

// Attach a hook.
func Attach(kind string, callback func(any)) {
	if !enabled {
		return
	}
	hookMu.Lock()
	defer hookMu.Unlock()
	hookMap[kind] = append(hookMap[kind], callback)
}

func callbacks(kind string) []func(any) {
	hookMu.Lock()
	defer hookMu.Unlock()
	return hookMap[kind]
}
