package statehamt

import (
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/gamesguru/rezzy/rezzy-ffi/go/rezzy"
)

func toEntries(entries []Entry) []rezzy.HAMTEntry {
	out := make([]rezzy.HAMTEntry, len(entries))
	for i, entry := range entries {
		out[i] = rezzy.HAMTEntry{entry.Type, entry.StateKey, entry.EventID}
	}
	return out
}

func fromEntries(entries []rezzy.HAMTEntry) []Entry {
	out := make([]Entry, len(entries))
	for i, entry := range entries {
		out[i] = Entry{Type: entry[0], StateKey: entry[1], EventID: entry[2]}
	}
	return out
}

func toNodes(nodes []Node) []rezzy.HAMTNode {
	out := make([]rezzy.HAMTNode, len(nodes))
	for i, node := range nodes {
		out[i] = rezzy.HAMTNode{Hash: hex.EncodeToString(node.Hash), Bytes: node.Bytes}
	}
	return out
}

func fromNodes(nodes []rezzy.HAMTNode) ([]Node, error) {
	out := make([]Node, len(nodes))
	for i, node := range nodes {
		hash, err := hex.DecodeString(node.Hash)
		if err != nil {
			return nil, fmt.Errorf("statehamt: decode node hash: %w", err)
		}
		out[i] = Node{Hash: hash, Bytes: node.Bytes}
	}
	return out, nil
}

// Build persists a state HAMT and returns every record needed to store it.
func Build(roomID string, entries []Entry) (*Persisted, error) {
	persisted, err := rezzy.StateHAMTPersist(roomID, toEntries(entries))
	if err != nil {
		return nil, err
	}
	rootHash, err := hex.DecodeString(persisted.RootHash)
	if err != nil {
		return nil, fmt.Errorf("statehamt: decode root hash: %w", err)
	}
	stateGroupID, err := hex.DecodeString(persisted.StateGroupID)
	if err != nil {
		return nil, fmt.Errorf("statehamt: decode state group id: %w", err)
	}
	nodes, err := fromNodes(persisted.Nodes)
	if err != nil {
		return nil, err
	}
	return &Persisted{
		RootHash:     rootHash,
		StateGroupID: stateGroupID,
		RootRecord:   persisted.RootRecord,
		Nodes:        nodes,
		EntryCount:   persisted.EntryCount,
		NodeBytes:    persisted.NodeBytes,
	}, nil
}

// Needed returns the node hashes required to walk the tree but absent from
// nodes.
func Needed(roomID string, rootHash []byte, nodes []Node) ([][]byte, error) {
	hashes, err := rezzy.StateHAMTNeeded(roomID, hex.EncodeToString(rootHash), toNodes(nodes))
	if err != nil {
		return nil, err
	}
	out := make([][]byte, len(hashes))
	for i, hash := range hashes {
		decoded, err := hex.DecodeString(hash)
		if err != nil {
			return nil, fmt.Errorf("statehamt: decode needed hash: %w", err)
		}
		out[i] = decoded
	}
	return out, nil
}

// Entries reads every leaf under rootHash from the supplied nodes.
func Entries(roomID string, rootHash []byte, nodes []Node) ([]Entry, error) {
	entries, err := rezzy.StateHAMTReadEntries(roomID, hex.EncodeToString(rootHash), toNodes(nodes))
	if err != nil {
		var missing *rezzy.MissingNodesError
		if errors.As(err, &missing) {
			hashes := make([][]byte, len(missing.Hashes))
			for i, hash := range missing.Hashes {
				decoded, decodeErr := hex.DecodeString(hash)
				if decodeErr != nil {
					return nil, fmt.Errorf("statehamt: decode missing hash: %w", decodeErr)
				}
				hashes[i] = decoded
			}
			return nil, &MissingNodesError{Hashes: hashes}
		}
		return nil, err
	}
	return fromEntries(entries), nil
}

// Lookup resolves keys under rootHash from the supplied nodes.
func Lookup(roomID string, rootHash []byte, nodes []Node, keys []Key) ([]string, []bool, error) {
	converted := make([]rezzy.HAMTNodeKey, len(keys))
	for i, key := range keys {
		converted[i] = rezzy.HAMTNodeKey{EventType: key.Type, StateKey: key.StateKey}
	}
	return rezzy.StateHAMTLookupRoot(roomID, hex.EncodeToString(rootHash), toNodes(nodes), converted)
}

// Verify checks a root record and its node set.
func Verify(roomID string, rootRecord []byte, nodes []Node) (*Verification, error) {
	verified, err := rezzy.StateHAMTVerify(roomID, rootRecord, toNodes(nodes))
	if err != nil {
		return nil, err
	}
	rootHash, err := hex.DecodeString(verified.RootHash)
	if err != nil {
		return nil, fmt.Errorf("statehamt: decode verified root hash: %w", err)
	}
	stateGroupID, err := hex.DecodeString(verified.StateGroupID)
	if err != nil {
		return nil, fmt.Errorf("statehamt: decode verified state group id: %w", err)
	}
	return &Verification{
		RootHash:     rootHash,
		StateGroupID: stateGroupID,
		RoomID:       verified.RoomID,
		Prefix:       verified.Prefix,
		EntryCount:   verified.EntryCount,
	}, nil
}
