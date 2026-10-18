package ha

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sirupsen/logrus"

	"codefloe.com/pat-s/zendrite/internal/httputil"
	"codefloe.com/pat-s/zendrite/setup/config"
)

// statusReadHeaderTimeout bounds probe connections, which are never long-lived.
const statusReadHeaderTimeout = 5 * time.Second

// listenStatus binds the candidate status listener.
//
// It is separate from the Matrix listeners on purpose. Those are bound only
// during activation, so a standby would otherwise answer nothing: its liveness
// probe would fail and Kubernetes would restart a healthy candidate, and its
// metrics would be unscrapable until it happened to be promoted.
func listenStatus(cfg config.HA, status *Status, metrics *config.Metrics) (net.Listener, func(), error) {
	mux := http.NewServeMux()
	mux.Handle("/_zendrite/monitor/", status.Handler())
	if metrics != nil && metrics.Enabled {
		// Refresh before serving: phase, transition and duration metrics are
		// derived from observed transitions, so a scrape that does not look
		// would report whatever the last probe happened to see.
		scrape := promhttp.Handler()
		mux.Handle("GET /metrics", httputil.WrapHandlerInBasicAuth(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			status.Refresh()
			scrape.ServeHTTP(w, r)
		}), metrics.BasicAuth))
	}
	listener, err := net.Listen("tcp", cfg.StatusAddress)
	if err != nil {
		return nil, nil, fmt.Errorf("bind HA status listener on %q: %w", cfg.StatusAddress, err)
	}
	server := &http.Server{Handler: mux, ReadHeaderTimeout: statusReadHeaderTimeout}
	return listener, func() {
		// Only the listener is closed on shutdown, so Serve returns
		// net.ErrClosed rather than http.ErrServerClosed. Logging that as an
		// error made every clean shutdown look like a failure.
		if err := server.Serve(listener); err != nil &&
			!errors.Is(err, http.ErrServerClosed) && !errors.Is(err, net.ErrClosed) {
			logrus.WithError(err).Error("HA status listener failed")
		}
	}, nil
}
