//go:build !(rezzy && linux && cgo)

package statehamt

// Enabled reports whether this build persists state HAMTs.
const Enabled = false

func Build(string, []Entry) (*Persisted, error)       { return nil, ErrDisabled }
func Needed(string, []byte, []Node) ([][]byte, error) { return nil, ErrDisabled }
func Entries(string, []byte, []Node) ([]Entry, error) { return nil, ErrDisabled }
func Lookup(string, []byte, []Node, []Key) ([]string, []bool, error) {
	return nil, nil, ErrDisabled
}
func Verify(string, []byte, []Node) (*Verification, error) { return nil, ErrDisabled }
