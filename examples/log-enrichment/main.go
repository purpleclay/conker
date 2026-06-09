// Example: log-enrichment
//
// Demonstrates stream.Stream: enrich a continuous stream of web server log
// entries concurrently — resolving each client IP to a country — while
// preserving the original chronological order in the output.
//
// The enrichment step has random latency (simulating a remote geolocation
// lookup), so the live output shows entries completing out of order. The
// final summary always reflects the original log sequence, making the
// ordering guarantee of stream.Stream directly observable.
//
// Run: go run . -workers=5 -run-for=5s
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/purpleclay/conker/stream"
)

const summaryRows = 15

var (
	workers = flag.Int("workers", 5, "maximum concurrent enrichments")
	runFor  = flag.Duration("run-for", 5*time.Second, "how long to process log entries")
)

// LogEntry holds a single enriched web server log line.
type LogEntry struct {
	Method  string
	Path    string
	Status  int
	IP      string
	Country string
	Latency time.Duration
}

var endpoints = []struct{ method, path string }{
	{"GET", "/api/users"},
	{"POST", "/api/orders"},
	{"GET", "/api/products"},
	{"PUT", "/api/profile"},
	{"GET", "/api/search"},
	{"POST", "/api/auth/login"},
	{"DELETE", "/api/cart/item"},
	{"GET", "/api/recommendations"},
}

// statuses skews toward 2xx to reflect realistic traffic.
var statuses = []int{200, 200, 200, 200, 201, 204, 301, 400, 401, 404, 429, 500}

func main() {
	flag.Parse()

	if *workers <= 0 {
		fmt.Fprintln(os.Stderr, "-workers must be > 0")
		flag.Usage()
		os.Exit(2)
	}
	if *runFor <= 0 {
		fmt.Fprintln(os.Stderr, "-run-for must be > 0")
		flag.Usage()
		os.Exit(2)
	}

	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, nil)))

	timeoutCtx, timeoutCancel := context.WithTimeout(context.Background(), *runFor)
	defer timeoutCancel()
	ctx, stop := signal.NotifyContext(timeoutCtx, os.Interrupt)
	defer stop()

	s := stream.New().WithMaxGoroutines(*workers)

	slog.Info("enrichment started", "workers", *workers, "run_for", *runFor)

	resolver := newStubResolver()
	var entries []LogEntry
	var submitted int

	for i := 0; ; i++ {
		ep := endpoints[rand.IntN(len(endpoints))]
		ip := randomIP()
		status := statuses[rand.IntN(len(statuses))]
		idx := i

		if err := s.GoCtx(ctx, func(taskCtx context.Context) stream.Callback {
			// Enrichment runs concurrently — completions arrive out of order.
			entry := enrich(taskCtx, resolver, ep.method, ep.path, status, ip)
			slog.Info(
				"enriched",
				"idx", idx,
				"ip", entry.IP,
				"country", entry.Country,
				"status", entry.Status,
				"latency", entry.Latency.Round(time.Millisecond),
			)
			// Callback runs serially in submission order.
			return func() { entries = append(entries, entry) }
		}); err != nil {
			slog.Info("processing stopped", "submitted", submitted, "reason", err)
			break
		}
		submitted++
	}

	s.Wait()
	printSummary(entries, submitted)
}

func enrich(ctx context.Context, r *stubResolver, method, path string, status int, ip string) LogEntry {
	start := time.Now()
	country := r.Lookup(ctx, ip)
	return LogEntry{
		Method:  method,
		Path:    path,
		Status:  status,
		IP:      ip,
		Country: country,
		Latency: time.Since(start),
	}
}

func printSummary(entries []LogEntry, submitted int) {
	fmt.Fprint(os.Stderr, "\n── Note: live enrichments above completed in a different order.\n")
	fmt.Fprint(os.Stderr, "── The table below is the same entries in original log order.\n\n")

	fmt.Fprintf(os.Stderr, " %-4s  %-6s  %-24s  %-3s  %-15s  %-11s  %s\n",
		"#", "method", "path", "st", "ip", "country", "latency")
	fmt.Fprintf(os.Stderr, " %s  %s  %s  %s  %s  %s  %s\n",
		strings.Repeat("─", 4), strings.Repeat("─", 6),
		strings.Repeat("─", 24), strings.Repeat("─", 3),
		strings.Repeat("─", 15), strings.Repeat("─", 11),
		strings.Repeat("─", 8))

	shown := min(summaryRows, len(entries))
	for i, e := range entries[:shown] {
		fmt.Fprintf(os.Stderr, " %-4d  %-6s  %-24s  %-3d  %-15s  %-11s  %s\n",
			i, e.Method, e.Path, e.Status, e.IP, e.Country,
			e.Latency.Round(time.Millisecond))
	}
	if len(entries) > summaryRows {
		fmt.Fprintf(os.Stderr, " ... (%d of %d shown)\n", summaryRows, len(entries))
	}

	fmt.Fprintf(os.Stderr, "\n submitted: %d   enriched: %d\n\n", submitted, len(entries))
}

// ─── Stub geolocation resolver ──────────────────────────────────────────────
//
// IPs are from the IANA documentation ranges (RFC 5737: 192.0.2.0/24,
// 198.51.100.0/24, 203.0.113.0/24). These are reserved for examples and
// do not belong to any real network; the country assignments are arbitrary
// labels chosen for illustration only.

var ipPool = []struct{ ip, country string }{
	{"203.0.113.1", "USA"},
	{"198.51.100.2", "Germany"},
	{"192.0.2.3", "France"},
	{"203.0.113.42", "Japan"},
	{"198.51.100.87", "UK"},
	{"192.0.2.156", "Canada"},
	{"203.0.113.201", "Australia"},
	{"198.51.100.33", "Brazil"},
	{"192.0.2.77", "Netherlands"},
	{"203.0.113.99", "Singapore"},
}

func randomIP() string {
	return ipPool[rand.IntN(len(ipPool))].ip
}

type stubResolver struct {
	lookup map[string]string
}

func newStubResolver() *stubResolver {
	m := make(map[string]string, len(ipPool))
	for _, entry := range ipPool {
		m[entry.ip] = entry.country
	}
	return &stubResolver{lookup: m}
}

func (r *stubResolver) Lookup(ctx context.Context, ip string) string {
	// Simulate variable network latency: uniform 50–300ms.
	delay := time.Duration(rand.IntN(251)+50) * time.Millisecond
	select {
	case <-time.After(delay):
	case <-ctx.Done():
		return "Unknown"
	}
	if country, ok := r.lookup[ip]; ok {
		return country
	}
	return "Unknown"
}
