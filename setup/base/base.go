// Copyright 2024 New Vector Ltd.
// Copyright 2020 The Matrix.org Foundation C.I.C.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package base

import (
	"bytes"
	"context"
	"crypto/tls"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"io/fs"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/gamesguru/gomatrixserverlib/fclient"
	sentryhttp "github.com/getsentry/sentry-go/http"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sirupsen/logrus"

	"codefloe.com/pat-s/zendrite/internal"
	"codefloe.com/pat-s/zendrite/internal/httputil"
	"codefloe.com/pat-s/zendrite/setup/config"
	"codefloe.com/pat-s/zendrite/setup/process"
)

//go:embed static/*.gotmpl
var staticContent embed.FS

//go:embed static/client/login
var loginFallback embed.FS
var StaticContent = staticContent

const HTTPServerTimeout = time.Minute * 5

const httpShutdownTimeout = 5 * time.Second

// CreateClient creates a new client (normally used for media fetch requests).
// Should only be called once per component.
func CreateClient(cfg *config.Zendrite, dnsCache *fclient.DNSCache) *fclient.Client {
	return createClient(cfg, dnsCache, true)
}

// CreateIdentityClient connects directly to identity servers without federation discovery.
func CreateIdentityClient(cfg *config.Zendrite) *fclient.Client {
	return createClient(cfg, nil, false)
}

func createClient(cfg *config.Zendrite, dnsCache *fclient.DNSCache, federationDiscovery bool) *fclient.Client {
	if cfg.Global.DisableFederation {
		return fclient.NewClient(
			fclient.WithTransport(noOpHTTPTransport),
		)
	}
	opts := []fclient.ClientOption{
		fclient.WithSkipVerify(cfg.FederationAPI.DisableTLSValidation),
		fclient.WithWellKnownSRVLookups(federationDiscovery),
	}
	if cfg.Global.DNSCache.Enabled && dnsCache != nil {
		opts = append(opts, fclient.WithDNSCache(dnsCache))
	}
	client := fclient.NewClient(opts...)
	client.SetUserAgent(fmt.Sprintf("Zendrite/%s", internal.VersionString()))
	return client
}

// CreateFederationClient creates a new federation client. Should only be called
// once per component.
func CreateFederationClient(cfg *config.Zendrite, dnsCache *fclient.DNSCache) fclient.FederationClient {
	identities := cfg.Global.SigningIdentities()
	if cfg.Global.DisableFederation {
		return fclient.NewFederationClient(
			identities, fclient.WithTransport(noOpHTTPTransport),
		)
	}
	opts := []fclient.ClientOption{
		fclient.WithTimeout(time.Minute * 5), //nolint:mnd
		fclient.WithSkipVerify(cfg.FederationAPI.DisableTLSValidation),
		fclient.WithKeepAlives(!cfg.FederationAPI.DisableHTTPKeepalives),
		fclient.WithUserAgent(fmt.Sprintf("Zendrite/%s", internal.VersionString())),
		fclient.WithAllowDenyNetworks(cfg.FederationAPI.AllowNetworkCIDRs, cfg.FederationAPI.DenyNetworkCIDRs),
	}
	if cfg.Global.DNSCache.Enabled {
		opts = append(opts, fclient.WithDNSCache(dnsCache))
	}
	client := fclient.NewFederationClient(
		identities, opts...,
	)
	return client
}

func ConfigureAdminEndpoints(processContext *process.ProcessContext, routers httputil.Routers) {
	routers.ZendriteAdmin.HandleFunc("/monitor/up", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}).Methods(http.MethodGet)
	routers.ZendriteAdmin.HandleFunc("/monitor/health", func(w http.ResponseWriter, r *http.Request) {
		if isDegraded, reasons := processContext.IsDegraded(); isDegraded {
			w.WriteHeader(http.StatusServiceUnavailable)
			_ = json.NewEncoder(w).Encode(struct {
				Warnings []string `json:"warnings"`
			}{
				Warnings: reasons,
			})
			return
		}
		w.WriteHeader(http.StatusOK)
	}).Methods(http.MethodGet)
}

// SetupAndServeHTTP sets up the HTTP server to serve client & federation APIs
// and adds a prometheus handler under /_zendrite/metrics.
func SetupAndServeHTTP(
	processContext *process.ProcessContext,
	cfg *config.Zendrite,
	routers httputil.Routers,
	externalHTTPAddr config.ServerAddress,
	certFile, keyFile *string,
) {
	if err := StartHTTP(processContext, cfg, routers, externalHTTPAddr, certFile, keyFile); err != nil {
		logrus.WithError(err).Error("Failed to start HTTP listener")
		processContext.ShutdownZendrite()
	}
	<-processContext.WaitForShutdown()
}

// StartHTTP binds synchronously so activation can report listener failures.
// All requests and listener shutdown are joined before dependency cleanup.
func StartHTTP(
	processContext *process.ProcessContext,
	cfg *config.Zendrite,
	routers httputil.Routers,
	externalHTTPAddr config.ServerAddress,
	certFile, keyFile *string, ready ...func() bool,
) error {
	externalRouter := httputil.NewRouter("")

	externalServ := &http.Server{
		Addr:         externalHTTPAddr.Address,
		WriteTimeout: HTTPServerTimeout,
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if len(ready) > 0 && !alwaysAdmitted(r.URL.Path) && !ready[0]() {
				http.Error(w, "Homeserver is not ready", http.StatusServiceUnavailable)
				return
			}
			if !processContext.Serve(func() { externalRouter.ServeHTTP(w, r) }) {
				http.Error(w, "Homeserver is stopping", http.StatusServiceUnavailable)
			}
		}),
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

	ConfigureAdminEndpoints(processContext, routers)

	// Parse and execute the landing page template
	tmpl := template.Must(template.ParseFS(staticContent, "static/*.gotmpl"))
	landingPage := &bytes.Buffer{}
	if err := tmpl.ExecuteTemplate(landingPage, "index.gotmpl", map[string]string{
		"Version": internal.VersionString(),
	}); err != nil {
		return fmt.Errorf("execute landing page template: %w", err)
	}

	routers.Static.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(landingPage.Bytes())
	}).Methods(http.MethodGet)

	// We only need the files beneath the static/client/login folder.
	sub, err := fs.Sub(loginFallback, "static/client/login")
	if err != nil {
		logrus.Panicf("unable to read embedded files, this should never happen: %s", err)
	}
	// Serve a static page for login fallback
	routers.Static.PathPrefix("/client/login/").Handler(http.StripPrefix("/_matrix/static/client/login/", http.FileServer(http.FS(sub))))

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
	externalRouter.PathPrefix(httputil.ZendriteAdminPathPrefix).Handler(routers.ZendriteAdmin)
	externalRouter.PathPrefix(httputil.PublicClientPathPrefix).Handler(clientHandler)
	if !cfg.Global.DisableFederation {
		externalRouter.PathPrefix(httputil.PublicKeyPathPrefix).Handler(routers.Keys)
		externalRouter.PathPrefix(httputil.PublicFederationPathPrefix).Handler(federationHandler)
	}
	externalRouter.PathPrefix(httputil.SynapseAdminPathPrefix).Handler(routers.SynapseAdmin)
	externalRouter.PathPrefix(httputil.PublicMediaPathPrefix).Handler(routers.Media)
	externalRouter.PathPrefix(httputil.PublicWellKnownPrefix).Handler(routers.WellKnown)
	externalRouter.PathPrefix(httputil.PublicStaticPath).Handler(routers.Static)

	if !externalHTTPAddr.Enabled() {
		return nil
	}
	if err := processContext.Context().Err(); err != nil {
		return err
	}
	var tlsConfig *tls.Config
	if certFile != nil && keyFile != nil {
		certificate, err := tls.LoadX509KeyPair(*certFile, *keyFile)
		if err != nil {
			return fmt.Errorf("load listener TLS certificate: %w", err)
		}
		tlsConfig = &tls.Config{Certificates: []tls.Certificate{certificate}, MinVersion: tls.VersionTLS12}
	}
	if externalHTTPAddr.IsUnixSocket() {
		if err := os.Remove(externalHTTPAddr.Address); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("remove unix socket: %w", err)
		}
	}
	listener, err := net.Listen(externalHTTPAddr.Network(), externalHTTPAddr.Address)
	if err != nil {
		return fmt.Errorf("bind HTTP listener: %w", err)
	}
	if externalHTTPAddr.IsUnixSocket() {
		if err = os.Chmod(externalHTTPAddr.Address, externalHTTPAddr.UnixSocketPermission); err != nil {
			_ = listener.Close()
			return fmt.Errorf("set unix socket permissions: %w", err)
		}
	}
	if tlsConfig != nil {
		externalServ.TLSConfig = tlsConfig
	}
	if !processContext.Go(func() {
		logrus.Infof("Starting external listener on %s", listener.Addr())
		var serveErr error
		if tlsConfig != nil {
			serveErr = externalServ.ServeTLS(listener, "", "")
		} else {
			serveErr = externalServ.Serve(listener)
		}
		if err := serveErr; err != nil && !errors.Is(err, http.ErrServerClosed) {
			logrus.WithError(err).Error("HTTP listener failed")
			processContext.ShutdownZendrite()
		}
	}) {
		_ = listener.Close()
		return processContext.Context().Err()
	}
	// Registration must also succeed if cancellation raced with Serve startup.
	// Use ComponentStarted here because this runs inside tracked application startup.
	processContext.ComponentStarted()
	go func() {
		defer processContext.ComponentFinished()
		<-processContext.WaitForShutdown()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), httpShutdownTimeout)
		defer cancel()
		if err := externalServ.Shutdown(shutdownCtx); err != nil {
			_ = externalServ.Close()
		}
	}()
	return nil
}

func WaitForShutdown(processCtx *process.ProcessContext) {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-sigs:
	case <-processCtx.WaitForShutdown():
	}
	signal.Reset(syscall.SIGINT, syscall.SIGTERM)

	logrus.Warnf("Shutdown signal received")

	processCtx.ShutdownZendrite()
	processCtx.WaitForComponentsToFinish()

	logrus.Warnf("Zendrite is exiting now")
}

// alwaysAdmitted reports whether a path must answer even while application
// admission is closed.
//
// Probes and metrics describe the process rather than serve Matrix traffic.
// Refusing them would blind monitoring and make a candidate look dead to every
// probe at the moment an operator most needs to see why it stopped admitting.
func alwaysAdmitted(path string) bool {
	return path == "/metrics" || strings.HasPrefix(path, httputil.ZendriteAdminPathPrefix+"monitor/")
}
