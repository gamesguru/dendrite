// Copyright 2025 Shane Jaroch
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package input

import (
	"slices"
	"testing"
)

func TestWindowOrder(t *testing.T) {
	ev := func(id string, depth int64, prevs ...string) windowEvent {
		return windowEvent{id: id, depth: depth, prevIDs: prevs}
	}
	toMap := func(evs ...windowEvent) map[string]windowEvent {
		m := make(map[string]windowEvent, len(evs))
		for _, e := range evs {
			m[e.id] = e
		}
		return m
	}
	ids := func(evs []windowEvent) []string {
		var out []string
		for _, e := range evs {
			out = append(out, e.id)
		}
		return out
	}

	t.Run("a chain is ordered by its prev events", func(t *testing.T) {
		// Depth is advisory: C claims to be shallower than B.
		got := ids(windowOrder(toMap(ev("$c", 1, "$b"), ev("$b", 5, "$a"), ev("$a", 9, "$join"))))
		if want := []string{"$a", "$b", "$c"}; !slices.Equal(got, want) {
			t.Fatalf("got %v, want %v", got, want)
		}
	})

	t.Run("a fork and merge puts the merge last", func(t *testing.T) {
		got := ids(windowOrder(toMap(
			ev("$a", 2, "$join"), ev("$b", 3, "$a"), ev("$c", 3, "$a"), ev("$d", 4, "$b", "$c"),
		)))
		if want := []string{"$a", "$b", "$c", "$d"}; !slices.Equal(got, want) {
			t.Fatalf("got %v, want %v", got, want)
		}
	})

	t.Run("a cycle gives no order", func(t *testing.T) {
		if got := windowOrder(toMap(ev("$a", 1, "$b"), ev("$b", 2, "$a"))); got != nil {
			t.Fatalf("got %v, want nil", ids(got))
		}
	})

	t.Run("an empty window is empty", func(t *testing.T) {
		if got := windowOrder(nil); len(got) != 0 {
			t.Fatalf("got %v, want none", ids(got))
		}
	})
}
