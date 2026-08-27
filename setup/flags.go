// Copyright 2024 New Vector Ltd.
// Copyright 2020 The Matrix.org Foundation C.I.C.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package setup

import (
	"flag"
	"fmt"
	"os"

	"github.com/sirupsen/logrus"

	"codefloe.com/pat-s/zendrite/internal"
	"codefloe.com/pat-s/zendrite/setup/config"
)

var (
	configPath                            = flag.String("config", "zendrite.yaml", "The path to the config file. For more information, see the config file in this repository.")
	version                               = flag.Bool("version", false, "Shows the current version and exits immediately.")
	enableRegistrationWithoutVerification = flag.Bool("really-enable-open-registration", false, "This allows open registration without secondary verification (reCAPTCHA). This is NOT RECOMMENDED and will SIGNIFICANTLY increase the risk that your server will be used to send spam or conduct attacks, which may result in your server being banned from rooms.")
)

// ParseFlags parses the commandline flags and uses them to create a config.
func ParseFlags(monolith bool) *config.Zendrite {
	flag.Parse()

	if *version {
		fmt.Println(internal.VersionString())
		os.Exit(0)
	}

	if *configPath == "" {
		logrus.Fatal("--config must be supplied")
	}

	// Fall back to the legacy config filename if the default was not
	// overridden and the new file does not exist.
	if *configPath == "zendrite.yaml" {
		if _, err := os.Stat("zendrite.yaml"); os.IsNotExist(err) {
			if _, err := os.Stat("dendrite.yaml"); err == nil {
				logrus.Warn("Using legacy config file dendrite.yaml — consider renaming it to zendrite.yaml")
				*configPath = "dendrite.yaml"
			}
		}
	}

	cfg, err := config.Load(*configPath)
	if err != nil {
		logrus.Fatalf("Invalid config file: %s", err)
	}

	if *enableRegistrationWithoutVerification {
		cfg.ClientAPI.OpenRegistrationWithoutVerificationEnabled = true
	}

	return cfg
}
