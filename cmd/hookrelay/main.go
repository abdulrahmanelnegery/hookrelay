// Command hookrelay runs the webhook delivery service: the HTTP API and the
// delivery worker pool in one process, sharing one Postgres pool.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/abdulrahman-muhammed-07/hookrelay/internal/api"
	"github.com/abdulrahman-muhammed-07/hookrelay/internal/backoff"
	"github.com/abdulrahman-muhammed-07/hookrelay/internal/config"
	"github.com/abdulrahman-muhammed-07/hookrelay/internal/store"
	"github.com/abdulrahman-muhammed-07/hookrelay/internal/worker"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	if err := run(log); err != nil {
		log.Error("fatal", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := store.Migrate(ctx, cfg.DatabaseURL); err != nil {
		return err
	}

	st, err := store.New(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer st.Close()

	log.Info("starting",
		"http_addr", cfg.HTTPAddr,
		"workers", cfg.Workers,
		"max_attempts", cfg.MaxAttempts,
		"retry_plan", planStrings(cfg),
	)

	pool := worker.New(st, cfg, log)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		pool.Run(ctx)
	}()

	srv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           api.New(st, cfg, log),
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("http server stopped", "err", err)
			stop()
		}
	}()

	<-ctx.Done()
	log.Info("shutting down", "grace", cfg.ShutdownGrace.String())

	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownGrace)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Error("http shutdown", "err", err)
	}
	wg.Wait()
	return nil
}

// planStrings renders the retry schedule for the startup log line.
func planStrings(cfg config.Config) []string {
	plan := backoff.Schedule(cfg.BackoffBase, cfg.BackoffFactor, cfg.BackoffCap, cfg.MaxAttempts-1)
	out := make([]string, len(plan))
	for i, d := range plan {
		out[i] = d.String()
	}
	return out
}
