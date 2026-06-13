// Example: background-services
//
// Demonstrates the daemon package: run a small set of long-lived background
// workers ("daemons") for a service, then shut them down gracefully when the
// run ends — either after -run-for elapses or on SIGINT.
//
// Three daemons are spawned:
//
//   - metrics: ticks periodically, logging a running counter. Stops as soon
//     as its context is cancelled.
//   - cache-refresher: ticks periodically, refreshing a cache. Occasionally a
//     refresh panics on a corrupted snapshot — daemon.Pool recovers it and
//     reports it from Stop as *panics.Recovered, without taking down the
//     other daemons.
//   - connection-drainer: on shutdown, drains in-flight connections. With
//     -slow-drain, draining takes longer than -shutdown-timeout, so Stop
//     returns context.DeadlineExceeded before draining finishes — the
//     daemon keeps running, but this example exits without waiting for it.
//
// Run: go run . -run-for=5s -shutdown-timeout=2s
// Run: go run . -run-for=5s -shutdown-timeout=2s -slow-drain
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"os"
	"os/signal"
	"time"

	"github.com/purpleclay/conker/daemon"
	"github.com/purpleclay/conker/panics"
)

var (
	runFor          = flag.Duration("run-for", 5*time.Second, "how long the service runs before shutting down")
	shutdownTimeout = flag.Duration("shutdown-timeout", 2*time.Second, "bound on how long graceful shutdown is allowed to take")
	slowDrain       = flag.Bool("slow-drain", false, "make connection draining exceed -shutdown-timeout")
)

var errCacheCorrupt = errors.New("cache snapshot corrupted")

func main() {
	flag.Parse()

	if *runFor <= 0 {
		fmt.Fprintln(os.Stderr, "-run-for must be > 0")
		flag.Usage()
		os.Exit(2)
	}
	if *shutdownTimeout <= 0 {
		fmt.Fprintln(os.Stderr, "-shutdown-timeout must be > 0")
		flag.Usage()
		os.Exit(2)
	}

	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, nil)))

	timeoutCtx, timeoutCancel := context.WithTimeout(context.Background(), *runFor)
	defer timeoutCancel()
	ctx, stop := signal.NotifyContext(timeoutCtx, os.Interrupt)
	defer stop()

	d := daemon.New(ctx)

	slog.Info("service started", "run_for", *runFor, "shutdown_timeout", *shutdownTimeout, "slow_drain", *slowDrain)

	d.Spawn(metricsReporter)
	d.Spawn(cacheRefresher)
	d.Spawn(connectionDrainer(*slowDrain))

	<-ctx.Done()
	slog.Info("shutdown requested", "reason", context.Cause(ctx))

	stopCtx, stopCancel := context.WithTimeout(context.Background(), *shutdownTimeout)
	defer stopCancel()

	start := time.Now()
	err := d.Stop(stopCtx)
	elapsed := time.Since(start).Round(time.Millisecond)

	switch {
	case err == nil:
		slog.Info("shutdown complete", "elapsed", elapsed)
	case errors.Is(err, context.DeadlineExceeded):
		slog.Warn("shutdown deadline exceeded; connection-drainer may not have finished draining", "elapsed", elapsed)
	case errors.Is(err, panics.ErrPanic):
		slog.Error("shutdown complete; one or more daemons panicked", "elapsed", elapsed, "err", err)
	default:
		slog.Error("shutdown completed with errors", "elapsed", elapsed, "err", err)
	}
}

// metricsReporter logs a running counter every tick, and stops as soon as
// ctx is cancelled.
func metricsReporter(ctx context.Context) {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	var count int
	for {
		select {
		case <-ctx.Done():
			slog.Info("metrics: stopped", "ticks", count)
			return
		case <-ticker.C:
			count++
			slog.Info("metrics: tick", "count", count)
		}
	}
}

// cacheRefresher refreshes a cache every tick. ~15% of refreshes hit a
// corrupted snapshot and panic — daemon.Pool recovers this and reports it
// from Stop, but the other daemons are unaffected.
func cacheRefresher(ctx context.Context) {
	ticker := time.NewTicker(700 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			slog.Info("cache-refresher: stopped")
			return
		case <-ticker.C:
			if rand.IntN(100) < 15 {
				panic(errCacheCorrupt)
			}
			slog.Info("cache-refresher: refreshed")
		}
	}
}

// connectionDrainer waits for shutdown, then drains in-flight connections.
// With slow set, draining takes longer than -shutdown-timeout.
func connectionDrainer(slow bool) func(context.Context) {
	return func(ctx context.Context) {
		<-ctx.Done()

		drain := 300 * time.Millisecond
		if slow {
			drain = 3 * time.Second
		}

		slog.Info("connection-drainer: draining in-flight connections", "estimated", drain)
		time.Sleep(drain)
		slog.Info("connection-drainer: drained")
	}
}
