// Package eventhash optionally makes rezzy authoritative for event IDs and
// content hash verdicts by installing gomatrixserverlib hooks, and cross-checks
// roomserver state snapshots against rezzy's HAMT.
//
// The default build links rezzy through cgo and installs the hooks that make
// it authoritative. The "rezzy" tag also enables the state HAMT cross-check.
// Both need the native library; see the build-rezzy and test-rezzy targets in
// the Makefile.
package eventhash
