// Package eventhash makes rezzy authoritative for event IDs and content hash
// verdicts by installing gomatrixserverlib hooks at import time, and
// cross-checks roomserver state snapshots against rezzy's HAMT.
//
// Every build links rezzy through cgo, installs the authoritative hooks, and
// compiles in the state HAMT cross-check; no build tag opts in or out. The
// package needs the native library, so build it with the rezzy-native target
// in the Makefile.
package eventhash
