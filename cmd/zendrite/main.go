// Copyright 2024 New Vector Ltd.
// Copyright 2017 Vector Creations Ltd
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/getsentry/sentry-go"
	"github.com/kardianos/minwinsvc"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"

	"codefloe.com/pat-s/zendrite/internal"
	"codefloe.com/pat-s/zendrite/setup"
	basepkg "codefloe.com/pat-s/zendrite/setup/base"
	"codefloe.com/pat-s/zendrite/setup/config"
	"codefloe.com/pat-s/zendrite/setup/ha"
	"codefloe.com/pat-s/zendrite/setup/lifecycle"
	"codefloe.com/pat-s/zendrite/setup/process"
	"codefloe.com/pat-s/zendrite/setup/profiling"
)

const shutdownTimeout = 30 * time.Second

var (
	unixSocket = flag.String("unix-socket", "",
		"EXPERIMENTAL(unstable): The HTTP listening unix socket for the server (disables http[s]-bind-address feature)",
	)
	unixSocketPermission = flag.String("unix-socket-permission", "755",
		"EXPERIMENTAL(unstable): The HTTP listening unix socket permission for the server (in chmod format like 755)",
	)
	httpBindAddr  = flag.String("http-bind-address", ":8008", "The HTTP listening port for the server")
	httpsBindAddr = flag.String("https-bind-address", ":8448", "The HTTPS listening port for the server")
	certFile      = flag.String("tls-cert", "", "The PEM formatted X509 certificate to use for TLS")
	keyFile       = flag.String("tls-key", "", "The PEM private key to use for TLS")
)

func main() {
	cfg := setup.ParseFlags(true)
	httpAddr := config.ServerAddress{}
	httpsAddr := config.ServerAddress{}
	if *unixSocket == "" {
		http, err := config.HTTPAddress("http://" + *httpBindAddr)
		if err != nil {
			logrus.WithError(err).Fatalf("Failed to parse http address")
		}
		httpAddr = http
		https, err := config.HTTPAddress("https://" + *httpsBindAddr)
		if err != nil {
			logrus.WithError(err).Fatalf("Failed to parse https address")
		}
		httpsAddr = https
	} else {
		socket, err := config.UnixSocketAddress(*unixSocket, *unixSocketPermission)
		if err != nil {
			logrus.WithError(err).Fatalf("Failed to parse unix socket")
		}
		httpAddr = socket
	}

	configErrors := &config.ConfigErrors{}
	cfg.Verify(configErrors)
	if len(*configErrors) > 0 {
		for _, err := range *configErrors {
			logrus.Errorf("Configuration error: %s", err)
		}
		logrus.Fatalf("Failed to start due to configuration errors")
	}

	internal.SetupStdLogging()
	internal.SetupHookLogging(cfg.Logging)
	internal.SetupPprof()

	basepkg.PlatformSanityChecks()

	logrus.Infof("Zendrite version %s", internal.VersionString())
	if !cfg.ClientAPI.RegistrationDisabled && cfg.ClientAPI.OpenRegistrationWithoutVerificationEnabled {
		logrus.Warn("Open registration is enabled")
	}

	// setup tracing
	closer, err := cfg.SetupTracing()
	if err != nil {
		logrus.WithError(err).Panicf("failed to start tracing")
	}
	defer closer.Close()

	profiler, err := profiling.Start(cfg.Global.Pyroscope)
	if err != nil {
		logrus.WithError(err).Panic("Failed to start continuous profiling")
	}
	if profiler != nil {
		logrus.Info("Pyroscope continuous profiling enabled")
		defer func() {
			profiler.Flush(true)
			if err := profiler.Stop(); err != nil {
				logrus.WithError(err).Warn("Failed to stop continuous profiling")
			}
		}()
	}

	// setup sentry
	if cfg.Global.Sentry.Enabled {
		logrus.Info("Setting up Sentry for debugging...")
		err = sentry.Init(sentry.ClientOptions{
			Dsn:              cfg.Global.Sentry.DSN,
			Environment:      cfg.Global.Sentry.Environment,
			Debug:            true,
			ServerName:       string(cfg.Global.ServerName),
			Release:          "zendrite@" + internal.VersionString(),
			AttachStacktrace: true,
		})
		if err != nil {
			logrus.WithError(err).Panic("failed to start Sentry")
		}
		defer sentry.Flush(time.Second * 5) //nolint:mnd // Existing telemetry flush budget.
	}

	upCounter := prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "zendrite",
		Name:      "up",
		ConstLabels: map[string]string{
			"version": internal.VersionString(),
		},
	})
	upCounter.Add(1)
	prometheus.MustRegister(upCounter)

	// Standalone authorizes activation immediately; HA requires election and
	// resource-enforced ownership. The interface is left nil when HA is
	// disabled so a typed nil pointer cannot pose as an installed gate.
	var gate ha.OwnershipGate
	var ownershipGate *ha.PostgresGate
	if cfg.Global.HA.Enabled {
		ownershipGate = ha.NewPostgresGate(string(cfg.Global.DatabaseOptions.ConnectionString), cfg.Global.HA)
		gate = ownershipGate
	}
	var server *lifecycle.Homeserver
	server = lifecycle.New(func(application *process.ProcessContext) error {
		return startHomeserver(application, cfg, httpAddr, httpsAddr, server.Ready, ownershipGate)
	}, shutdownTimeout)
	ownership, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	minwinsvc.SetOnExit(func() { stop(); server.Stop() })
	if err := ha.SetCompatibility(cfg); err != nil {
		logrus.WithError(err).Fatal("HA compatibility validation failed")
	}
	if err := ha.Run(ownership, cfg.Global.HA, &cfg.Global.Metrics, server, gate, shutdownTimeout); err != nil {
		logrus.WithError(err).Error("Homeserver stopped")
		os.Exit(1) //nolint:gocritic // A stuck application must exit immediately; deferred profiling shutdown may also block.
	}
}
