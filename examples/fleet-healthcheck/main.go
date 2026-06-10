// Example: fleet-healthcheck
//
// Demonstrates the iter package: concurrently health-check a fixed fleet of
// hosts using iter.MapSeqErr, returning results in the original fleet order
// regardless of completion order.
//
// Unlike the etl-pipeline and log-enrichment examples, the input here is a
// known, bounded slice rather than a continuous stream — the natural fit for
// iter's slice and iterator-based API.
//
// One host ("auth-03.internal") always returns a fatal DNS error. With
// -fail-fast, that error cancels the derived context passed to all in-flight
// checks via WithCancelOnError, and dispatch of any remaining hosts stops.
// Without -fail-fast, every host is checked regardless of individual
// failures.
//
// Run: go run . -workers=5 -fail-fast
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
	"slices"
	"strings"
	"time"

	"github.com/purpleclay/conker/iter"
)

var (
	workers  = flag.Int("workers", 5, "maximum concurrent health checks")
	timeout  = flag.Duration("timeout", 5*time.Second, "overall deadline for the health check run")
	failFast = flag.Bool("fail-fast", false, "cancel remaining checks on the first fatal error")
)

var fleet = []string{
	"api-01.internal", "api-02.internal", "api-03.internal",
	"web-01.internal", "web-02.internal", "web-03.internal", "web-04.internal",
	"db-01.internal", "db-02.internal",
	"cache-01.internal", "cache-02.internal",
	"auth-01.internal", "auth-02.internal", "auth-03.internal", "auth-04.internal",
	"queue-01.internal", "queue-02.internal",
	"worker-01.internal", "worker-02.internal", "worker-03.internal",
}

const fatalHost = "auth-03.internal"

var (
	errFatalDNS = errors.New("DNS resolution permanently failed")
	errDegraded = errors.New("degraded: high latency reported")
)

// HealthResult holds the outcome of checking one host.
type HealthResult struct {
	Status  string
	Latency time.Duration
}

func main() {
	flag.Parse()

	if *workers <= 0 {
		fmt.Fprintln(os.Stderr, "-workers must be > 0")
		flag.Usage()
		os.Exit(2)
	}
	if *timeout <= 0 {
		fmt.Fprintln(os.Stderr, "-timeout must be > 0")
		flag.Usage()
		os.Exit(2)
	}

	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, nil)))

	timeoutCtx, timeoutCancel := context.WithTimeout(context.Background(), *timeout)
	defer timeoutCancel()
	ctx, stop := signal.NotifyContext(timeoutCtx, os.Interrupt)
	defer stop()

	slog.Info("healthcheck started", "hosts", len(fleet), "workers", *workers, "fail_fast", *failFast)

	options := []iter.Option{iter.WithMaxGoroutines(*workers), iter.WithContext(ctx)}
	if *failFast {
		options = append(options, iter.WithCancelOnError())
	}

	checker := newStubChecker()
	results, err := iter.MapSeqErr(slices.Values(fleet), func(taskCtx context.Context, host string) (HealthResult, error) {
		return checkHost(taskCtx, checker, host)
	}, options...)

	printSummary(results, err)
}

// checkHost runs the health check for a single host and logs the outcome as
// it completes — completions arrive in a different order than the fleet
// slice, which the final summary corrects.
func checkHost(ctx context.Context, c *stubChecker, host string) (HealthResult, error) {
	start := time.Now()
	status, err := c.Check(ctx, host)
	latency := time.Since(start)
	if err != nil {
		slog.Warn("unhealthy", "host", host, "latency", latency.Round(time.Millisecond), "err", err)
		return HealthResult{}, fmt.Errorf("%s: %w", host, err)
	}
	slog.Info("healthy", "host", host, "latency", latency.Round(time.Millisecond))
	return HealthResult{Status: status, Latency: latency}, nil
}

func printSummary(results []HealthResult, err error) {
	fmt.Fprint(os.Stderr, "\n── Note: live checks above completed in a different order.\n")
	fmt.Fprint(os.Stderr, "── The table below is the same hosts in original fleet order.\n\n")

	fmt.Fprintf(os.Stderr, " %-20s  %-9s  %s\n", "host", "status", "latency")
	fmt.Fprintf(os.Stderr, " %s  %s  %s\n",
		strings.Repeat("─", 20), strings.Repeat("─", 9), strings.Repeat("─", 8))

	for i, r := range results {
		host := fleet[i]
		if r.Status == "" {
			fmt.Fprintf(os.Stderr, " %-20s  %-9s  -\n", host, "error")
			continue
		}
		fmt.Fprintf(os.Stderr, " %-20s  %-9s  %s\n", host, r.Status, r.Latency.Round(time.Millisecond))
	}
	if skipped := len(fleet) - len(results); skipped > 0 {
		fmt.Fprintf(os.Stderr, " ... %d host(s) skipped (cancelled before dispatch)\n", skipped)
	}

	fmt.Fprintf(os.Stderr, "\n checked: %d/%d\n", len(results), len(fleet))
	if err != nil {
		fmt.Fprintf(os.Stderr, "\n errors:\n%s\n", err)
	}
	fmt.Fprintln(os.Stderr)
}

type stubChecker struct{}

func newStubChecker() *stubChecker { return &stubChecker{} }

// Check simulates a network health check with variable latency. The fleet's
// fatalHost always fails with errFatalDNS; a fraction of the remaining hosts
// fail with errDegraded to exercise error aggregation.
func (c *stubChecker) Check(ctx context.Context, host string) (string, error) {
	delay := time.Duration(rand.IntN(131)+20) * time.Millisecond // 20–150ms
	select {
	case <-time.After(delay):
	case <-ctx.Done():
		return "", ctx.Err()
	}

	if host == fatalHost {
		return "", errFatalDNS
	}
	if rand.IntN(100) < 15 {
		return "", errDegraded
	}
	return "healthy", nil
}
