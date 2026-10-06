package ha

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// Contract declares which builds may take over one HA deployment from each
// other. It replaces exact executable identity as the version-admission gate:
// two different builds may share a deployment only where this contract says
// they can, and setup/ha/testdata/levels.json records, for every level, the
// shared-state surface that the level was qualified against.
//
// The deployment's identity, which is its configuration, signing keys,
// storage targets and enabled MSCs, is not part of the contract. It must stay
// identical across a rolling upgrade and is checked separately.
type Contract struct {
	// Protocol versions ownership, fencing and the compatibility record
	// itself. Candidates of different protocols never share a deployment.
	Protocol int `json:"protocol"`
	// Level identifies this build's shared-state surface: the PostgreSQL
	// schema and migrations it runs, and the JetStream streams and message
	// formats it reads and writes. It increases with every change to that
	// surface, and TestContractSurface refuses a changed surface under an
	// unchanged level.
	Level int `json:"level"`
	// UpgradeFrom is the oldest level this build can take over from. Its
	// migrations and consumers must accept every state a build of that level
	// or later leaves behind, including messages still queued in JetStream.
	UpgradeFrom int `json:"upgrade_from"`
	// RollbackTo is the oldest level that may take over from state this build
	// has written. It rises above an older level when this build makes a
	// change that the older one cannot read, such as dropping a column it
	// uses or publishing a message it cannot decode.
	RollbackTo int `json:"rollback_to"`
}

// release is this build's contract.
//
// Raise Level for every change to the shared-state surface. Keep UpgradeFrom
// and RollbackTo at the previous release's level while every change is
// backward compatible: additive migrations, new tables, new optional message
// fields. Raise UpgradeFrom when this build cannot take over from an older
// one, and RollbackTo when an older build cannot take over from this one;
// either turns the transition into an offline upgrade.
var release = Contract{Protocol: 1, Level: releaseLevel, UpgradeFrom: 1, RollbackTo: 1}

// releaseLevel is the current shared-state surface level. Level 2 stores
// per-thread notification counts, level 3 custom profile fields, level 4
// case-folds stored email 3PIDs, and level 5 adds refresh tokens to devices.
const releaseLevel = 5

// contractOverride replaces the release contract in qualification builds,
// which need a second build at another level to exercise rolling upgrades
// and refusals. It is only set at link time, as
// "-X codefloe.com/pat-s/zendrite/setup/ha.contractOverride=<value>", never by
// configuration. The value is protocol:level:upgrade_from:rollback_to, or
// one relative to the release contract: "adjacent" is the next level, which
// upgrades from and rolls back to this one, and "unadmitted" is two levels
// ahead and upgrades only from itself.
var contractOverride string

// Release reports this build's contract.
func Release() Contract {
	switch contractOverride {
	case "":
		return release
	case "adjacent":
		return Contract{Protocol: release.Protocol, Level: release.Level + 1, UpgradeFrom: release.Level, RollbackTo: release.Level}
	case "unadmitted":
		const ahead = 2
		level := release.Level + ahead
		return Contract{Protocol: release.Protocol, Level: level, UpgradeFrom: level, RollbackTo: level}
	}
	c, err := parseContract(contractOverride)
	if err != nil {
		panic(fmt.Sprintf("invalid HA contract override %q: %v", contractOverride, err))
	}
	return c
}

func parseContract(value string) (Contract, error) {
	fields := strings.Split(value, ":")
	const parts = 4
	if len(fields) != parts {
		return Contract{}, errors.New("expected protocol:level:upgrade_from:rollback_to")
	}
	numbers := make([]int, parts)
	for i, field := range fields {
		n, err := strconv.Atoi(field)
		if err != nil {
			return Contract{}, err
		}
		numbers[i] = n
	}
	c := Contract{Protocol: numbers[0], Level: numbers[1], UpgradeFrom: numbers[2], RollbackTo: numbers[3]}
	return c, c.Validate()
}

// Validate rejects a contract that cannot describe a real build.
func (c Contract) Validate() error {
	if c.Protocol < 1 || c.UpgradeFrom < 1 || c.RollbackTo < 1 {
		return errors.New("protocol, upgrade_from and rollback_to must be positive")
	}
	if c.UpgradeFrom > c.Level || c.RollbackTo > c.Level {
		return errors.New("upgrade_from and rollback_to must not exceed level")
	}
	return nil
}

func (c Contract) String() string {
	return fmt.Sprintf("protocol %d, level %d (upgrades from level %d, rolls back to level %d)", c.Protocol, c.Level, c.UpgradeFrom, c.RollbackTo)
}

// ErrIncompatibleBuild reports that this build may not take over the
// deployment as it stands. The running owner is unaffected; the transition
// needs the offline upgrade procedure instead.
var ErrIncompatibleBuild = errors.New("HA upgrade contract does not admit this build; an offline upgrade is required")

// Recorded is the deployment's compatibility record, written by every
// activation before it runs a migration.
type Recorded struct {
	// Fingerprint is the deployment identity. An empty fingerprint means no
	// activation has written a record yet.
	Fingerprint string
	// Protocol and Level are those of the build that activated last.
	Protocol int
	Level    int
	// Floor is the oldest level that may still take over. Storage keeps every
	// migration a build ran, so the floor only rises: it is the highest
	// RollbackTo of every build that has activated.
	Floor int
	// Version is the last activated build's version string, for operators.
	Version string
}

// Empty reports whether no activation has recorded compatibility yet.
func (r Recorded) Empty() bool { return r.Fingerprint == "" }

// Admit reports whether a build with this contract and deployment identity
// may take over from the recorded state.
//
// A newer build must be able to upgrade from the level that wrote last, whose
// messages may still be queued. Any build must be at or above the floor, which
// covers every schema change made so far. A build at the recorded level or
// between the floor and it is a rollback that the recorded builds allow.
func (c Contract) Admit(fingerprint string, recorded Recorded) error {
	if recorded.Empty() {
		return nil
	}
	if recorded.Fingerprint != fingerprint {
		return fmt.Errorf("%w: the configuration differs from the running deployment's (signing keys, storage targets, stream namespace, search or MSCs), or the record was written by a build without an upgrade contract", ErrIncompatibleBuild)
	}
	if recorded.Level < 1 || recorded.Floor < 1 {
		return fmt.Errorf("%w: the compatibility record has no upgrade level", ErrIncompatibleBuild)
	}
	if recorded.Protocol != c.Protocol {
		return fmt.Errorf("%w: this build uses HA protocol %d and the deployment uses protocol %d", ErrIncompatibleBuild, c.Protocol, recorded.Protocol)
	}
	if c.Level < recorded.Floor {
		return fmt.Errorf("%w: this build is at level %d, and the deployment's storage no longer supports levels below %d", ErrIncompatibleBuild, c.Level, recorded.Floor)
	}
	if c.Level > recorded.Level && c.UpgradeFrom > recorded.Level {
		return fmt.Errorf("%w: this build upgrades only from level %d or later, and the deployment is at level %d", ErrIncompatibleBuild, c.UpgradeFrom, recorded.Level)
	}
	return nil
}

// Next reports the record this build writes when it activates.
func (c Contract) Next(fingerprint, version string, recorded Recorded) Recorded {
	return Recorded{Fingerprint: fingerprint, Protocol: c.Protocol, Level: c.Level, Floor: max(recorded.Floor, c.RollbackTo), Version: version}
}
