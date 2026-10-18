package sync

import (
	"errors"
	"net/http/httptest"
	"testing"

	"codefloe.com/pat-s/zendrite/syncapi/types"
)

func TestStateAfterKey(t *testing.T) {
	t.Parallel()
	tests := []struct {
		query   string
		want    string
		wantErr bool
	}{
		{query: "", want: ""},
		{query: "use_state_after=false", want: ""},
		{query: "use_state_after=true", want: types.StateAfterKey},
		{query: "org.matrix.msc4222.use_state_after=true", want: types.UnstableStateAfterKey},
		{query: "org.matrix.msc4222.use_state_after=false", want: ""},
		// The stable parameter wins over the unstable one.
		{query: "use_state_after=true&org.matrix.msc4222.use_state_after=true", want: types.StateAfterKey},
		{query: "use_state_after=1", wantErr: true},
		{query: "org.matrix.msc4222.use_state_after=yes", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.query, func(t *testing.T) {
			t.Parallel()
			got, err := stateAfterKey(httptest.NewRequest("GET", "/sync?"+tt.query, nil))
			if tt.wantErr {
				if !errors.Is(err, errInvalidParam) {
					t.Fatalf("got error %v want errInvalidParam", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got != tt.want {
				t.Fatalf("got key %q want %q", got, tt.want)
			}
		})
	}
}
