// Command hubd runs the automation hub: it loads the config, opens the
// database, serves the HTTP API and processes the job queue until it receives
// SIGINT or SIGTERM.
//
//	hubd [-config path]
//
// The config path is, in order of precedence: the -config flag, $HUB_CONFIG,
// then ./hub.yaml. See hub.example.yaml.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/djenkins-1/automation-hub/internal/api"
	"github.com/djenkins-1/automation-hub/internal/app"
	"github.com/djenkins-1/automation-hub/internal/config"
)

// httpShutdownGrace is how long in-flight API requests get to finish at
// shutdown. The queue worker has its own grace period for in-flight jobs.
const httpShutdownGrace = 10 * time.Second

func main() { os.Exit(realMain()) }

func realMain() int {
	var configFlag string
	flag.StringVar(&configFlag, "config", "", "path to the YAML config (default: $HUB_CONFIG, else hub.yaml)")
	flag.Parse()
	if flag.NArg() > 0 {
		fmt.Fprintf(os.Stderr, "hubd: unexpected argument %q\n", flag.Arg(0))
		return 2
	}

	// Until the config is read there is no logger to configure, so early
	// failures are plain lines on stderr.
	cfg, err := config.Load(configPath(configFlag))
	if err != nil {
		fmt.Fprintf(os.Stderr, "hubd: %v\n", err)
		return 1
	}
	if cfg.Server.Token == "" {
		fmt.Fprintln(os.Stderr, "hubd: server.token is required; set it in the config, for example: token: ${HUB_TOKEN}")
		return 1
	}
	log := app.NewLogger(cfg.Log, os.Stderr)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	// After the first signal, go back to default handling so a second Ctrl-C
	// ends the process at once. That is the way out if a drain hangs.
	context.AfterFunc(ctx, stop)

	if err := run(ctx, cfg, log); err != nil {
		log.Error("hubd failed", "err", err)
		return 1
	}
	log.Info("hubd stopped")
	return 0
}

func configPath(flagValue string) string {
	if flagValue != "" {
		return flagValue
	}
	if v := os.Getenv("HUB_CONFIG"); v != "" {
		return v
	}
	return "hub.yaml"
}

// run starts the queue worker and the HTTP server and blocks until ctx is
// cancelled (a signal) or either component stops by itself. It returns nil
// after a clean, signal-initiated shutdown.
func run(ctx context.Context, cfg *config.Config, log *slog.Logger) error {
	a, err := app.New(ctx, cfg, app.WithLogger(log))
	if err != nil {
		return err
	}
	// Deferred first so it runs last: the database is closed only after both
	// the HTTP server and the worker have stopped using it.
	defer func() {
		if err := a.Close(); err != nil {
			log.Warn("closing database", "err", err)
		}
	}()

	handler, err := api.New(a.Ingest(), cfg.Server.Token, log)
	if err != nil {
		return err
	}
	// Binding here, before anything starts, makes "address already in use" a
	// startup error and lets the log show the real address for ":0".
	ln, err := net.Listen("tcp", cfg.Server.Addr)
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	srv := &http.Server{
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       2 * time.Minute,
		ErrorLog:          slog.NewLogLogger(log.Handler(), slog.LevelWarn),
	}

	// The worker's context is not derived from ctx. At shutdown the HTTP
	// server stops first (no new events), and the worker keeps processing
	// jobs meanwhile; it is told to stop only once the server is down.
	workerCtx, stopWorker := context.WithCancel(context.WithoutCancel(ctx))
	defer stopWorker()

	workerDone := make(chan error, 1)
	go func() { workerDone <- a.Run(workerCtx) }()

	serverDone := make(chan error, 1)
	log.Info("api listening", "addr", ln.Addr().String())
	go func() { serverDone <- srv.Serve(ln) }()

	var firstErr error
	select {
	case <-ctx.Done():
		log.Info("shutdown requested")
	case err := <-serverDone: // Serve only returns on its own when it has failed
		serverDone = nil
		firstErr = fmt.Errorf("http server: %w", err)
	case err := <-workerDone:
		workerDone = nil
		if err == nil {
			err = errors.New("stopped unexpectedly")
		}
		firstErr = fmt.Errorf("queue worker: %w", err)
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), httpShutdownGrace)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Warn("http shutdown incomplete; closing remaining connections", "err", err)
		_ = srv.Close()
	}
	if serverDone != nil {
		<-serverDone // returns http.ErrServerClosed now that Shutdown has run
	}

	stopWorker()
	if workerDone != nil {
		if err := <-workerDone; err != nil && firstErr == nil {
			firstErr = fmt.Errorf("queue worker: %w", err)
		}
	}
	return firstErr
}
