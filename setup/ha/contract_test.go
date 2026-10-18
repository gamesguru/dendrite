package ha

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestContractAdmit(t *testing.T) {
	recorded := func(level, floor int) Recorded {
		return Recorded{Fingerprint: "deployment", Protocol: 1, Level: level, Floor: floor}
	}
	for _, tc := range []struct {
		name     string
		build    Contract
		recorded Recorded
		admitted bool
	}{
		{"first activation", Contract{Protocol: 1, Level: 3, UpgradeFrom: 3, RollbackTo: 3}, Recorded{}, true},
		{"same level", Contract{Protocol: 1, Level: 3, UpgradeFrom: 2, RollbackTo: 2}, recorded(3, 2), true},
		{"adjacent upgrade", Contract{Protocol: 1, Level: 4, UpgradeFrom: 3, RollbackTo: 3}, recorded(3, 2), true},
		{"skipped level", Contract{Protocol: 1, Level: 5, UpgradeFrom: 4, RollbackTo: 4}, recorded(3, 2), false},
		{"declared multi-level upgrade", Contract{Protocol: 1, Level: 5, UpgradeFrom: 3, RollbackTo: 4}, recorded(3, 2), true},
		{"rollback above the floor", Contract{Protocol: 1, Level: 3, UpgradeFrom: 2, RollbackTo: 2}, recorded(4, 3), true},
		{"rollback below the floor", Contract{Protocol: 1, Level: 2, UpgradeFrom: 2, RollbackTo: 2}, recorded(4, 3), false},
		// After a rollback to level 3, a level 4 build is back above the
		// last writer and must still be able to upgrade from it.
		{"upgrade again after rollback", Contract{Protocol: 1, Level: 4, UpgradeFrom: 3, RollbackTo: 3}, recorded(3, 3), true},
		{"protocol", Contract{Protocol: 2, Level: 3, UpgradeFrom: 3, RollbackTo: 3}, recorded(3, 2), false},
		{"deployment identity", Contract{Protocol: 1, Level: 3, UpgradeFrom: 3, RollbackTo: 3}, Recorded{Fingerprint: "other", Protocol: 1, Level: 3, Floor: 3}, false},
		{"record without an upgrade level", Contract{Protocol: 1, Level: 3, UpgradeFrom: 3, RollbackTo: 3}, Recorded{Fingerprint: "deployment"}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.build.Admit("deployment", tc.recorded)
			if tc.admitted {
				require.NoError(t, err)
				return
			}
			require.ErrorIs(t, err, ErrIncompatibleBuild)
		})
	}
}

// The floor only rises: storage keeps every migration a build ran, so a
// rollback must not readmit builds that an earlier activation excluded.
func TestContractNextKeepsTheFloor(t *testing.T) {
	upgraded := Contract{Protocol: 1, Level: 4, UpgradeFrom: 3, RollbackTo: 3}.Next("deployment", "v4", Recorded{Fingerprint: "deployment", Protocol: 1, Level: 3, Floor: 2})
	require.Equal(t, Recorded{Fingerprint: "deployment", Protocol: 1, Level: 4, Floor: 3, Version: "v4"}, upgraded)
	rolledBack := Contract{Protocol: 1, Level: 3, UpgradeFrom: 2, RollbackTo: 2}.Next("deployment", "v3", upgraded)
	require.Equal(t, 3, rolledBack.Level)
	require.Equal(t, 3, rolledBack.Floor)
	require.Error(t, Contract{Protocol: 1, Level: 2, UpgradeFrom: 2, RollbackTo: 2}.Admit("deployment", rolledBack))
}

func TestContractValidate(t *testing.T) {
	require.NoError(t, Release().Validate())
	for _, value := range []string{"1:2:3:1", "1:2:1:3", "0:1:1:1", "1:1:0:1", "1:1:1", "a:1:1:1"} {
		_, err := parseContract(value)
		require.Error(t, err, value)
	}
	c, err := parseContract("1:5:4:3")
	require.NoError(t, err)
	require.Equal(t, Contract{Protocol: 1, Level: 5, UpgradeFrom: 4, RollbackTo: 3}, c)
}

func TestContractOverride(t *testing.T) {
	defer func(previous string) { contractOverride = previous }(contractOverride)
	base := Release()
	contractOverride = "adjacent"
	adjacent := Release()
	require.Equal(t, base.Level+1, adjacent.Level)
	require.NoError(t, adjacent.Admit("deployment", base.Next("deployment", "", Recorded{})))
	contractOverride = "unadmitted"
	require.ErrorIs(t, Release().Admit("deployment", base.Next("deployment", "", Recorded{})), ErrIncompatibleBuild)
	contractOverride = "1:4:3:2"
	require.Equal(t, Contract{Protocol: 1, Level: 4, UpgradeFrom: 3, RollbackTo: 2}, Release())
}
