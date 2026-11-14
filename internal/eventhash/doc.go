// Package eventhash compares gomatrixserverlib event IDs and content hashes
// with rezzy, and cross-checks roomserver state snapshots against rezzy's HAMT.
//
// Builds without the "rezzy" tag get no-op checks, so the default build has no
// native dependency. Build with the tag to link rezzy; see the build-rezzy and
// test-rezzy targets in the Makefile.
package eventhash
