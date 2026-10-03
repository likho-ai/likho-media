// Package app puts the service together and runs it.
package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"time"

	"connectrpc.com/grpchealth"
	"github.com/likho-ai/likho-contracts/packages/go/gen/likho/media/v1/mediav1connect"

	"github.com/likho-ai/likho-media/internal/audio"
	"github.com/likho-ai/likho-media/internal/config"
	"github.com/likho-ai/likho-media/internal/events"
	"github.com/likho-ai/likho-media/internal/httpapi"
	"github.com/likho-ai/likho-media/internal/ingest"
	"github.com/likho-ai/likho-media/internal/links"
	"github.com/likho-ai/likho-media/internal/objects"
	"github.com/likho-ai/likho-media/internal/rpc"
	"github.com/likho-ai/likho-media/internal/store"
)

// Version of the service, shown in the start-up log line.
const Version = "0.1.0"

// App is the running service.
type App struct {
	cfg    config.Config
	log    *slog.Logger
	store  *store.Store
	bus    *events.Bus
	ingest *ingest.Processor

	httpListener net.Listener
	grpcListener net.Listener
	httpServer   *http.Server
	grpcServer   *http.Server
	health       *grpchealth.StaticChecker
}

// Options change how the service is put together. The zero value is the real thing.
type Options struct {
	// Publisher replaces the event bus as the place events are sent to (tests).
	Publisher func(real *events.Bus) ingest.Publisher
	// Timing of the conversion workers. The zero value means the defaults.
	Timing ingest.Timing
}

// New connects to everything the service needs and opens its ports. Nothing is served
// until Run is called.
func New(ctx context.Context, cfg config.Config, log *slog.Logger, options Options) (*App, error) {
	tools := audio.Tools{FFmpeg: cfg.FFmpegPath, FFprobe: cfg.FFprobePath}
	if err := tools.Check(ctx); err != nil {
		return nil, err
	}
	db, err := store.Open(ctx, cfg.DatabaseURL)
	if err != nil {
		return nil, err
	}
	fail := func(err error) (*App, error) {
		db.Close()
		return nil, err
	}
	if err := db.Migrate(ctx); err != nil {
		return fail(fmt.Errorf("database migration: %w", err))
	}
	objs, err := objects.Open(cfg.S3Endpoint, cfg.S3AccessKey, cfg.S3SecretKey, cfg.S3Region)
	if err != nil {
		return fail(err)
	}
	if err := objs.CheckBuckets(ctx, cfg.BucketOriginal, cfg.BucketPlayback, cfg.BucketPeaks); err != nil {
		return fail(err)
	}
	bus, err := events.Connect(ctx, cfg.NATSURL)
	if err != nil {
		return fail(err)
	}
	fail = func(err error) (*App, error) {
		bus.Close()
		db.Close()
		return nil, err
	}

	httpListener, err := net.Listen("tcp", fmt.Sprintf(":%d", cfg.HTTPPort))
	if err != nil {
		return fail(err)
	}
	grpcListener, err := net.Listen("tcp", fmt.Sprintf(":%d", cfg.GRPCPort))
	if err != nil {
		_ = httpListener.Close()
		return fail(err)
	}
	if cfg.PublicURL == "" { // tests: links point straight at this instance
		cfg.PublicURL = fmt.Sprintf("http://127.0.0.1:%d", httpListener.Addr().(*net.TCPAddr).Port)
	}

	var publisher ingest.Publisher = bus
	if options.Publisher != nil {
		publisher = options.Publisher(bus)
	}
	signer := links.NewSigner(cfg.LinkSecret, cfg.PublicURL)
	if cfg.InternalURL != "" {
		signer.SetInternalURL(cfg.InternalURL)
	}
	processor := ingest.New(cfg, db, objs, publisher, options.Timing, log)
	ready := func(ctx context.Context) bool { return bus.Connected() && db.Ping(ctx) == nil }
	api := httpapi.New(cfg, db, objs, signer, processor.Wake, ready, log)

	rpcMux := http.NewServeMux()
	rpcMux.Handle(mediav1connect.NewMediaServiceHandler(rpc.New(cfg, db, objs, signer, log)))
	health := grpchealth.NewStaticChecker(mediav1connect.MediaServiceName)
	rpcMux.Handle(grpchealth.NewHandler(health))

	// gRPC clients speak HTTP/2 without TLS inside the cluster.
	protocols := new(http.Protocols)
	protocols.SetHTTP1(true)
	protocols.SetUnencryptedHTTP2(true)

	return &App{
		cfg: cfg, log: log, store: db, bus: bus, ingest: processor,
		httpListener: httpListener,
		grpcListener: grpcListener,
		httpServer:   &http.Server{Handler: api.Handler(), ReadHeaderTimeout: 10 * time.Second},
		grpcServer:   &http.Server{Handler: rpcMux, Protocols: protocols, ReadHeaderTimeout: 10 * time.Second},
		health:       health,
	}, nil
}

// HTTPAddr is the address the HTTP side listens on ("127.0.0.1:4010").
func (a *App) HTTPAddr() string { return localAddr(a.httpListener) }

// GRPCAddr is the address the gRPC side listens on.
func (a *App) GRPCAddr() string { return localAddr(a.grpcListener) }

func localAddr(listener net.Listener) string {
	return fmt.Sprintf("127.0.0.1:%d", listener.Addr().(*net.TCPAddr).Port)
}

// Run serves until ctx is cancelled, then stops cleanly: requests in flight and the files
// being converted are finished first.
func (a *App) Run(ctx context.Context) error {
	failed := make(chan error, 2)
	serve := func(server *http.Server, listener net.Listener) {
		if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			failed <- err
		}
	}
	go serve(a.httpServer, a.httpListener)
	go serve(a.grpcServer, a.grpcListener)

	workCtx, stopWork := context.WithCancel(context.Background())
	var work sync.WaitGroup
	work.Go(func() { a.ingest.Run(workCtx) })

	a.log.Info(fmt.Sprintf("likho-media %s: HTTP on %s, gRPC on %s, %d workers",
		Version, a.httpListener.Addr(), a.grpcListener.Addr(), a.cfg.Workers))

	var runErr error
	select {
	case <-ctx.Done():
	case runErr = <-failed:
	}

	a.log.Info("stopping: finishing the work in hand")
	a.health.SetStatus(mediav1connect.MediaServiceName, grpchealth.StatusNotServing)
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	_ = a.httpServer.Shutdown(shutdownCtx)
	_ = a.grpcServer.Shutdown(shutdownCtx)
	stopWork()
	work.Wait()
	a.bus.Close()
	a.store.Close()
	return runErr
}
