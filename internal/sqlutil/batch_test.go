package sqlutil

import (
	"slices"
	"testing"
)

func TestLastByKey(t *testing.T) {
	type row struct{ key, value string }
	rows := []row{{"a", "1"}, {"b", "1"}, {"a", "2"}, {"c", "1"}, {"b", "2"}}
	got := LastByKey(rows, func(r row) string { return r.key })
	want := []row{{"a", "2"}, {"c", "1"}, {"b", "2"}}
	if !slices.Equal(got, want) {
		t.Fatalf("LastByKey = %v, want %v", got, want)
	}

	unique := []row{{"a", "1"}, {"b", "1"}}
	if got := LastByKey(unique, func(r row) string { return r.key }); !slices.Equal(got, unique) {
		t.Fatalf("LastByKey without duplicates = %v, want %v", got, unique)
	}
}
