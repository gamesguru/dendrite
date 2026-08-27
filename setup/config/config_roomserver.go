package config

import (
	"fmt"
	"strings"

	"codefloe.com/pat-s/gomatrixserverlib"
	"github.com/goccy/go-yaml"
	log "github.com/sirupsen/logrus"
)

// AutoPurgeMode controls when the roomserver auto-purges rooms with no
// local activity.
type AutoPurgeMode string

const (
	// AutoPurgeNever disables auto-purging.
	AutoPurgeNever AutoPurgeMode = "never"
	// AutoPurgeOnEmpty fires a purge as soon as no local user has a
	// joined membership in the room. Users with a leave-state membership
	// row are ignored, so their pre-leave history access via /messages
	// disappears with the room.
	AutoPurgeOnEmpty AutoPurgeMode = "on_empty"
	// AutoPurgeOnAllForgotten fires a purge once no local user has any
	// non-forgotten membership row. Users who left but did not /forget
	// keep their pre-leave history access until they explicitly forget.
	AutoPurgeOnAllForgotten AutoPurgeMode = "on_all_forgotten"
)

// UnmarshalYAML accepts either the tri-state string form (never, on_empty,
// on_all_forgotten) or a legacy bool (true → on_all_forgotten, false → never).
// An empty / unset value leaves the receiver untouched so that Defaults()
// can supply the production default.
func (m *AutoPurgeMode) UnmarshalYAML(data []byte) error {
	raw := strings.TrimSpace(string(data))
	if raw == "" || raw == "null" || raw == "~" {
		return nil
	}
	var asBool bool
	if err := yaml.Unmarshal(data, &asBool); err == nil {
		if asBool {
			*m = AutoPurgeOnAllForgotten
		} else {
			*m = AutoPurgeNever
		}
		return nil
	}
	var asStr string
	if err := yaml.Unmarshal(data, &asStr); err != nil {
		return fmt.Errorf("auto_purge_empty_rooms: must be %q, %q, %q, or a boolean; got %q",
			AutoPurgeNever, AutoPurgeOnEmpty, AutoPurgeOnAllForgotten, raw)
	}
	switch AutoPurgeMode(asStr) {
	case AutoPurgeNever, AutoPurgeOnEmpty, AutoPurgeOnAllForgotten:
		*m = AutoPurgeMode(asStr)
		return nil
	default:
		return fmt.Errorf("auto_purge_empty_rooms: must be %q, %q, %q, or a boolean; got %q",
			AutoPurgeNever, AutoPurgeOnEmpty, AutoPurgeOnAllForgotten, asStr)
	}
}

type RoomServer struct {
	Matrix *Global `yaml:"-"`

	DefaultRoomVersion gomatrixserverlib.RoomVersion `yaml:"default_room_version,omitempty"`

	// AutoPurgeMode controls automatic purging of rooms that no local user
	// is interested in. Three values are supported:
	//
	//   - "never": auto-purge is disabled. Administrators can still purge
	//     individual rooms via /admin/purgeRoom.
	//   - "on_empty": purge as soon as no local user has a joined membership
	//     in the room. Users with a leave-state row keep no history because
	//     the room is gone before they could call /messages.
	//   - "on_all_forgotten" (the default): purge once no local user has any
	//     non-forgotten membership row. Users who left but did not /forget
	//     keep their pre-leave history access until they explicitly forget.
	//
	// For backwards compatibility the YAML key auto_purge_empty_rooms also
	// accepts a boolean: true → on_all_forgotten, false → never.
	AutoPurgeMode AutoPurgeMode `yaml:"auto_purge_empty_rooms"`

	// AutoForgetOnLeave makes every transition to leave or ban for a local
	// user behave as if the user had also called /forget on the room. This
	// matches the Matrix 1.18 m.forget_forced_upon_leave capability, which
	// is advertised via /_matrix/client/v3/capabilities when this is true.
	// Independent of AutoPurgeMode: a forgotten room is not deleted, just
	// hidden from the leaving user's history.
	//
	// Defaults to false. The asymmetric trade-off is the reason: forgetting
	// a room drops the user's archived-rooms UI entry and their /messages
	// access to pre-leave history, which is worse for users who got kicked
	// or who left a high-activity room. Users who prefer the auto-forget UX
	// can opt in.
	AutoForgetOnLeave bool `yaml:"auto_forget_on_leave"`

	Database DatabaseOptions `yaml:"database,omitempty"`
}

// AutoPurgeEnabled returns true when auto-purge is configured to fire in
// at least one of its modes.
func (c *RoomServer) AutoPurgeEnabled() bool {
	return c.AutoPurgeMode == AutoPurgeOnEmpty || c.AutoPurgeMode == AutoPurgeOnAllForgotten
}

func (c *RoomServer) Defaults(opts DefaultOpts) {
	c.DefaultRoomVersion = gomatrixserverlib.RoomVersionV12
	c.AutoPurgeMode = AutoPurgeOnAllForgotten
	if opts.Generate {
		if !opts.SingleDatabase {
			c.Database.ConnectionString = "file:roomserver.db"
		}
	}
}

func (c *RoomServer) Verify(configErrs *ConfigErrors) {
	if c.Matrix.DatabaseOptions.ConnectionString == "" {
		checkNotEmpty(configErrs, "room_server.database.connection_string", string(c.Database.ConnectionString))
	}

	if !gomatrixserverlib.KnownRoomVersion(c.DefaultRoomVersion) {
		configErrs.Add(fmt.Sprintf("invalid value for config key 'room_server.default_room_version': unsupported room version: %q", c.DefaultRoomVersion))
	} else if !gomatrixserverlib.StableRoomVersion(c.DefaultRoomVersion) {
		log.Warnf("WARNING: Provided default room version %q is unstable", c.DefaultRoomVersion)
	}

	switch c.AutoPurgeMode {
	case AutoPurgeNever, AutoPurgeOnEmpty, AutoPurgeOnAllForgotten:
		// ok
	default:
		configErrs.Add(fmt.Sprintf(
			"invalid value for config key 'room_server.auto_purge_empty_rooms': must be %q, %q, %q, or a boolean; got %q",
			AutoPurgeNever, AutoPurgeOnEmpty, AutoPurgeOnAllForgotten, c.AutoPurgeMode,
		))
	}
}
