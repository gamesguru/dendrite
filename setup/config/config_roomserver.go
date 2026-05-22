package config

import (
	"fmt"

	"codefloe.com/pat-s/gomatrixserverlib"
	log "github.com/sirupsen/logrus"
)

type RoomServer struct {
	Matrix *Global `yaml:"-"`

	DefaultRoomVersion gomatrixserverlib.RoomVersion `yaml:"default_room_version,omitempty"`

	// AutoPurgeEmptyRooms enables automatic purging of empty rooms.
	// When true (the default), two behaviors fire together:
	//   - at startup, a one-shot sweep purges any rooms which currently have
	//     no local members; and
	//   - at runtime, whenever the last local member leaves a room, the room
	//     is scheduled for an asynchronous purge.
	// Set to false to disable both behaviors.
	AutoPurgeEmptyRooms bool `yaml:"auto_purge_empty_rooms"`

	Database DatabaseOptions `yaml:"database,omitempty"`
}

func (c *RoomServer) Defaults(opts DefaultOpts) {
	c.DefaultRoomVersion = gomatrixserverlib.RoomVersionV12
	c.AutoPurgeEmptyRooms = true
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
}
