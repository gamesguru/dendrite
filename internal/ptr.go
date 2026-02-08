package internal

import "cmp"

// PtrOrNil dereferences a pointer, returning the pointed-to value as any.
// If the pointer is nil, it returns nil. This is useful for passing nullable
// integer values to SQL query arguments or JSON serialization.
func PtrOrNil[T cmp.Ordered](in *T) any {
	if in == nil {
		return nil
	}
	return *in
}
