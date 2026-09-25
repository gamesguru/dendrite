package main

import (
	"fmt"

	"codefloe.com/pat-s/gomatrixserverlib/fclient"
	"github.com/sirupsen/logrus"

	"codefloe.com/pat-s/zendrite/appservice"
	"codefloe.com/pat-s/zendrite/federationapi"
	"codefloe.com/pat-s/zendrite/internal/caching"
	"codefloe.com/pat-s/zendrite/internal/httputil"
	"codefloe.com/pat-s/zendrite/internal/sqlutil"
	"codefloe.com/pat-s/zendrite/roomserver"
	"codefloe.com/pat-s/zendrite/setup"
	basepkg "codefloe.com/pat-s/zendrite/setup/base"
	"codefloe.com/pat-s/zendrite/setup/config"
	"codefloe.com/pat-s/zendrite/setup/ha"
	"codefloe.com/pat-s/zendrite/setup/jetstream"
	"codefloe.com/pat-s/zendrite/setup/mscs"
	"codefloe.com/pat-s/zendrite/setup/process"
	"codefloe.com/pat-s/zendrite/userapi"
)

// startHomeserver initializes all application dependencies only during activation.
func startHomeserver(processCtx *process.ProcessContext, cfg *config.Zendrite, httpAddr, httpsAddr config.ServerAddress, ready func() bool, gate *ha.PostgresGate) error {
	releaseStorage, err := ha.PrepareStorage(processCtx.Context(), cfg, gate)
	if err != nil {
		return err
	}
	defer releaseStorage()
	// create DNS cache
	var dnsCache *fclient.DNSCache
	if cfg.Global.DNSCache.Enabled {
		dnsCache = fclient.NewDNSCache(
			cfg.Global.DNSCache.CacheSize,
			cfg.Global.DNSCache.CacheLifetime,
			cfg.FederationAPI.AllowNetworkCIDRs,
			cfg.FederationAPI.DenyNetworkCIDRs,
		)
		logrus.Infof(
			"DNS cache enabled (size %d, lifetime %s)",
			cfg.Global.DNSCache.CacheSize,
			cfg.Global.DNSCache.CacheLifetime,
		)
	}

	federationClient := basepkg.CreateFederationClient(cfg, dnsCache)
	// A remote server cannot reject a retired owner, so outbound dispatch is
	// refused here instead, before a transaction starts its round-trip.
	if gate != nil {
		federationClient = basepkg.GuardFederationClient(federationClient, gate)
	}
	httpClient := basepkg.CreateClient(cfg, dnsCache)

	// prepare required dependencies
	// In HA every pooled connection is bound to the authorized activation, so
	// PostgreSQL refuses a retired owner even if it has not noticed the loss.
	var dbOptions []sqlutil.ConnectionOption
	if gate != nil {
		dbOptions = append(dbOptions, sqlutil.WithConnectionGuard(gate.ApplicationName(), gate.GuardConnection))
	}
	cm := sqlutil.NewConnectionManager(processCtx, cfg.Global.DatabaseOptions, dbOptions...)
	routers := httputil.NewRouters()

	caches := caching.NewRistrettoCache(cfg.Global.Cache.EstimatedMaxSize, cfg.Global.Cache.MaxAge, caching.EnableMetrics)
	processCtx.Defer(caches.Close)
	// A typed nil must not pose as an installed ownership reporter, so the
	// interface stays empty outside HA.
	natsInstance := jetstream.NATSInstance{}
	if gate != nil {
		natsInstance.Ownership = gate
	}
	if err := processCtx.Context().Err(); err != nil {
		return err
	}
	rsAPI := roomserver.NewInternalAPI(processCtx, cfg, cm, &natsInstance, caches, caching.EnableMetrics)
	if err := processCtx.Context().Err(); err != nil {
		return err
	}
	fsAPI, startFederationAPI := federationapi.NewUnstartedInternalAPI(
		processCtx, cfg, cm, &natsInstance, federationClient, rsAPI, caches, nil, false,
	)

	keyRing := fsAPI.KeyRing()

	// The underlying roomserver implementation needs to be able to call the fedsender.
	// This is different to rsAPI which can be the http client which doesn't need this
	// dependency. Other components also need updating after their dependencies are up.
	if err := processCtx.Context().Err(); err != nil {
		return err
	}
	rsAPI.SetFederationAPI(fsAPI, keyRing)
	startFederationAPI()

	if err := processCtx.Context().Err(); err != nil {
		return err
	}
	userAPI := userapi.NewInternalAPI(processCtx, cfg, cm, &natsInstance, rsAPI, federationClient, caching.EnableMetrics, fsAPI.IsBlacklistedOrBackingOff)
	if err := processCtx.Context().Err(); err != nil {
		return err
	}
	asAPI := appservice.NewInternalAPI(processCtx, cfg, &natsInstance, userAPI, rsAPI)

	rsAPI.SetAppserviceAPI(asAPI)
	rsAPI.SetUserAPI(userAPI)

	monolith := setup.Monolith{
		Config:    cfg,
		Client:    httpClient,
		FedClient: federationClient,
		KeyRing:   keyRing,

		AppserviceAPI: asAPI,
		// always use the concrete impl here even in -http mode because adding public routes
		// must be done on the concrete impl not an HTTP client else fedapi will call itself
		FederationAPI: fsAPI,
		RoomserverAPI: rsAPI,
		UserAPI:       userAPI,
	}
	if gate != nil {
		monolith.Owner = gate
	}
	if err := processCtx.Context().Err(); err != nil {
		return err
	}
	monolith.AddAllPublicRoutes(processCtx, cfg, routers, cm, &natsInstance, caches, caching.EnableMetrics)

	if err := processCtx.Context().Err(); err != nil {
		return err
	}
	if len(cfg.MSCs.MSCs) > 0 {
		if err := mscs.Enable(cfg, cm, routers, &monolith, caches); err != nil {
			return fmt.Errorf("enable MSCs: %w", err)
		}
	}

	if err := processCtx.Context().Err(); err != nil {
		return err
	}
	if cfg.Global.HA.Enabled {
		js, _ := natsInstance.Prepare(processCtx, &cfg.Global.JetStream)
		if err := jetstream.RecoverSync(processCtx.Context(), js, &cfg.Global.JetStream); err != nil {
			return err
		}
	}
	// Admission is evaluated per request so a candidate that loses ownership
	// rejects traffic immediately, including on established keep-alive
	// connections, rather than waiting for Kubernetes to remove its endpoint.
	admit := ready
	if gate != nil {
		admit = ha.Admit(ready, gate)
	}
	if err := basepkg.StartHTTP(processCtx, cfg, routers, httpAddr, nil, nil, admit); err != nil {
		return err
	}
	if *unixSocket == "" && *certFile != "" && *keyFile != "" {
		if err := basepkg.StartHTTP(processCtx, cfg, routers, httpsAddr, certFile, keyFile, admit); err != nil {
			return err
		}
	}
	return processCtx.Context().Err()
}
