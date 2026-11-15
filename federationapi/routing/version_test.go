package routing

import (
	"net/http"
	"testing"

	"codefloe.com/pat-s/zendrite/internal/reconciliation"
)

func TestVersionUnstableFeaturesFollowMSC4521(t *testing.T) {
	digestKey := "tk.nutra.msc4521.digest." + reconciliation.DigestType
	reconciliationKey := "tk.nutra.msc0501.reconciliation"

	for _, enabled := range []bool{true, false} {
		name := "disabled"
		if enabled {
			name = "enabled"
		}
		t.Run(name, func(t *testing.T) {
			res := Version(enabled)
			if res.Code != http.StatusOK {
				t.Fatalf("status = %d, want 200", res.Code)
			}
			v, ok := res.JSON.(*version)
			if !ok {
				t.Fatalf("JSON is %T, want *version", res.JSON)
			}
			_, hasDigest := v.UnstableFeatures[digestKey]
			_, hasReconciliation := v.UnstableFeatures[reconciliationKey]
			if !enabled {
				if hasDigest || hasReconciliation || len(v.UnstableFeatures) != 0 {
					t.Fatalf("unstable_features = %v, want none", v.UnstableFeatures)
				}
				return
			}
			if !hasDigest {
				t.Fatalf("missing unstable feature %q in %v", digestKey, v.UnstableFeatures)
			}
			if !hasReconciliation {
				t.Fatalf("missing unstable feature %q in %v", reconciliationKey, v.UnstableFeatures)
			}
			for key, value := range v.UnstableFeatures {
				if !value {
					t.Fatalf("unstable feature %q is false, want true", key)
				}
			}
		})
	}
}
