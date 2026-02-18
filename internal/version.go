package internal

import (
	"runtime/debug"
	"strings"
)

// Version can be set at build time using:
// -ldflags "-X codefloe.com/pat-s/zendrite/internal.version=1.0.0".
var version = "dev"

const (
	gitRevLen = 7 // 7 matches the displayed characters on github.com
)

func VersionString() string {
	return version
}

//nolint:gochecknoinits
func init() {
	// If version was set via ldflags, use it as-is but append git revision
	parts := []string{}

	// Try to get the revision Zendrite was built from.
	// If we can't, e.g. Zendrite wasn't built (go run) or no VCS version is present,
	// we just use the provided version above.
	info, ok := debug.ReadBuildInfo()
	if ok {
		for _, setting := range info.Settings {
			if setting.Key == "vcs.revision" {
				revLen := len(setting.Value)
				if revLen >= gitRevLen {
					parts = append(parts, setting.Value[:gitRevLen])
				} else {
					parts = append(parts, setting.Value[:revLen])
				}
				break
			}
		}
	}

	if len(parts) > 0 {
		version += "+" + strings.Join(parts, ".")
	}
}
