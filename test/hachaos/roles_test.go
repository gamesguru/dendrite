//go:build linux

package hachaos

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3afero"
	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/spf13/afero"
)

// runNATSRole runs one broker from a configuration file, as nats-server would.
func runNATSRole(stop <-chan os.Signal) error {
	opts, err := natsserver.ProcessConfigFile(os.Getenv("HACHAOS_CONFIG"))
	if err != nil {
		return fmt.Errorf("read NATS configuration: %w", err)
	}
	server, err := natsserver.NewServer(opts)
	if err != nil {
		return fmt.Errorf("create NATS server: %w", err)
	}
	server.ConfigureLogger()
	go server.Start()
	if !server.ReadyForConnections(time.Minute) {
		return errors.New("NATS server did not become ready")
	}
	<-stop
	server.Shutdown()
	server.WaitForShutdown()
	return nil
}

// runS3Role serves an S3 API backed by a directory, so an acknowledged upload
// survives a crash of this process.
func runS3Role(stop <-chan os.Signal) error {
	root := os.Getenv("HACHAOS_S3_ROOT")
	if err := os.MkdirAll(root, 0o755); err != nil {
		return err
	}
	backend, err := s3afero.MultiBucket(afero.NewBasePathFs(afero.NewOsFs(), root))
	if err != nil {
		return fmt.Errorf("open S3 store: %w", err)
	}
	listener, err := net.Listen("tcp4", os.Getenv("HACHAOS_S3_ADDRESS"))
	if err != nil {
		return err
	}
	server := &http.Server{Handler: gofakes3.New(backend, gofakes3.WithAutoBucket(true)).Server(), ReadHeaderTimeout: 10 * time.Second}
	go func() { _ = server.Serve(listener) }()
	<-stop
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return server.Shutdown(ctx)
}
