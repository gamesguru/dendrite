package sqlutil

// LastByKey returns items with every earlier item sharing a key with a later
// one removed, keeping the order of the survivors.
//
// A batched INSERT ... ON CONFLICT DO UPDATE fails when two rows of the
// statement conflict with each other, whereas the same rows written one
// statement at a time leave the last one in place. Deduplicating this way
// first gives a batched write the semantics of the row-by-row one.
func LastByKey[T any, K comparable](items []T, key func(T) K) []T {
	last := make(map[K]int, len(items))
	for i, item := range items {
		last[key(item)] = i
	}
	if len(last) == len(items) {
		return items
	}
	out := make([]T, 0, len(last))
	for i, item := range items {
		if last[key(item)] == i {
			out = append(out, item)
		}
	}
	return out
}
