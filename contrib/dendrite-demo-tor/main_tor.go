// Copyright 2024 New Vector Ltd.
// Copyright 2017 Vector Creations Ltd
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"embed"
	"net"
	"net/http"
	"net/url"
	"sync/atomic"
	"text/template"

	"github.com/cretz/bine/tor"
	sentryhttp "github.com/getsentry/sentry-go/http"
	"github.com/go-i2p/onramp"
	"github.com/kardianos/minwinsvc"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sirupsen/logrus"

	"codefloe.com/pat-s/dendrite/internal"
	"codefloe.com/pat-s/dendrite/internal/httputil"
	basepkg "codefloe.com/pat-s/dendrite/setup/base"
	"codefloe.com/pat-s/dendrite/setup/config"
	"codefloe.com/pat-s/dendrite/setup/process"
)

func start() (*tor.Tor, error) {
	if skip {
		return nil, nil
	}
	return tor.Start(context.Background(), nil)
}

func dialer() (*tor.Dialer, error) {
	if skip {
		return nil, nil
	}
	return t.Dialer(context.TODO(), nil)
}

var (
	t, terr        = start()
	tdialer, tderr = dialer()
)

// Dial either a unix socket address, or connect to a remote address over Tor. Always uses Tor.
func DialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	if terr != nil {
		return nil, terr
	}
	if (tderr != nil) || (tdialer == nil) {
		return nil, tderr
	}
	if network == "unix" {
		return net.Dial(network, addr)
	}
	// convert the addr to a full URL
	url, err := url.Parse(addr)
	if err != nil {
		return nil, err
	}
	return tdialer.DialContext(ctx, network, url.Host)
}

//go:embed static/*.gotmpl
var staticContent embed.FS

// SetupAndServeHTTPS sets up the HTTPS server to serve client & federation APIs
// and adds a prometheus handler under /_dendrite/metrics.
func SetupAndServeHTTPS(
	processContext *process.ProcessContext,
	cfg *config.Dendrite,
	routers httputil.Routers,
) {
	// create a transport that uses SAM to dial TCP Connections
	httpClient := &http.Client{
		Transport: &http.Transport{
			DialContext: DialContext,
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true,
			},
		},
	}

	http.DefaultClient = httpClient

	onion, err := onramp.NewOnion("dendrite-onion")
	if err != nil {
		logrus.WithError(err).Fatal("failed to create onion")
	}
	defer onion.Close()
	listener, err := onion.ListenTLS()
	if err != nil {
		logrus.WithError(err).Fatal("failed to serve HTTPS")
	}
	defer listener.Close()

	externalHTTPSAddr := config.ServerAddress{}
	https, err := config.HTTPAddress("https://" + listener.Addr().String())
	if err != nil {
		logrus.WithError(err).Fatalf("Failed to parse http address")
	}
	externalHTTPSAddr = https

	externalRouter := httputil.NewRouter("")

	externalServ := &http.Server{
		Addr:         externalHTTPSAddr.Address,
		WriteTimeout: basepkg.HTTPServerTimeout,
		Handler:      externalRouter,
		BaseContext: func(_ net.Listener) context.Context {
			return processContext.Context()
		},
	}

	// Redirect for Landing Page
	externalRouter.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, httputil.PublicStaticPath, http.StatusFound)
	}).Methods(http.MethodGet)

	if cfg.Global.Metrics.Enabled {
		externalRouter.Handle("/metrics", httputil.WrapHandlerInBasicAuth(promhttp.Handler(), cfg.Global.Metrics.BasicAuth)).Methods(http.MethodGet)
	}

	basepkg.ConfigureAdminEndpoints(processContext, routers)

	// Parse and execute the landing page template
	tmpl := template.Must(template.ParseFS(staticContent, "static/*.gotmpl"))
	landingPage := &bytes.Buffer{}
	if err := tmpl.ExecuteTemplate(landingPage, "index.gotmpl", map[string]string{
		"Version": internal.VersionString(),
	}); err != nil {
		logrus.WithError(err).Fatal("failed to execute landing page template")
	}

	routers.Static.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(landingPage.Bytes())
	}).Methods(http.MethodGet)

	var clientHandler http.Handler
	clientHandler = routers.Client
	if cfg.Global.Sentry.Enabled {
		sentryHandler := sentryhttp.New(sentryhttp.Options{
			Repanic: true,
		})
		clientHandler = sentryHandler.Handle(routers.Client)
	}
	var federationHandler http.Handler
	federationHandler = routers.Federation
	if cfg.Global.Sentry.Enabled {
		sentryHandler := sentryhttp.New(sentryhttp.Options{
			Repanic: true,
		})
		federationHandler = sentryHandler.Handle(routers.Federation)
	}
	externalRouter.PathPrefix(httputil.DendriteAdminPathPrefix).Handler(routers.DendriteAdmin)
	externalRouter.PathPrefix(httputil.PublicClientPathPrefix).Handler(clientHandler)
	if !cfg.Global.DisableFederation {
		externalRouter.PathPrefix(httputil.PublicKeyPathPrefix).Handler(routers.Keys)
		externalRouter.PathPrefix(httputil.PublicFederationPathPrefix).Handler(federationHandler)
	}
	externalRouter.PathPrefix(httputil.SynapseAdminPathPrefix).Handler(routers.SynapseAdmin)
	externalRouter.PathPrefix(httputil.PublicMediaPathPrefix).Handler(routers.Media)
	externalRouter.PathPrefix(httputil.PublicWellKnownPrefix).Handler(routers.WellKnown)
	externalRouter.PathPrefix(httputil.PublicStaticPath).Handler(routers.Static)

	externalRouter.SetNotFoundHandler(httputil.NotFoundCORSHandler)
	externalRouter.SetMethodNotAllowedHandler(httputil.NotAllowedHandler)

	if externalHTTPSAddr.Enabled() {
		go func() {
			var externalShutdown atomic.Bool // RegisterOnShutdown can be called more than once
			logrus.Infof("Starting external listener on https://%s", externalServ.Addr)
			processContext.ComponentStarted()
			externalServ.RegisterOnShutdown(func() {
				if externalShutdown.CompareAndSwap(false, true) {
					processContext.ComponentFinished()
					logrus.Infof("Stopped external HTTPS listener")
				}
			})
			addr := listener.Addr()
			externalServ.Addr = addr.String()
			if err := externalServ.Serve(listener); err != nil {
				if err != http.ErrServerClosed {
					logrus.WithError(err).Fatal("failed to serve HTTPS")
				}
			}

			logrus.Infof("Stopped external listener on %s", externalServ.Addr)
		}()
	}

	minwinsvc.SetOnExit(processContext.ShutdownDendrite)
	<-processContext.WaitForShutdown()

	logrus.Infof("Stopping HTTPS listeners")
	_ = externalServ.Shutdown(context.Background())
	logrus.Infof("Stopped HTTPS listeners")
}
