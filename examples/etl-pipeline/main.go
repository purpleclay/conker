// Example: etl-pipeline
//
// Demonstrates pool.ResultPool[T]: process a continuous stream of objects
// from a store with bounded concurrency, per-task timeouts, and
// submission-order results.
//
// Tasks have random latencies and a configurable failure rate so the live
// output shows completions arriving out of order, while the final summary
// always reflects the original submission sequence — the guarantee that
// distinguishes ResultPool from sourcegraph/conc and errgroup.
//
// The pipeline runs until -run-for elapses or SIGINT is received.
//
// Run: go run . -workers=5 -task-timeout=200ms -run-for=5s
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
	"strings"
	"time"

	"github.com/purpleclay/conker/pool"
)

const summaryRows = 15

var (
	workers     = flag.Int("workers", 5, "maximum concurrent tasks")
	taskTimeout = flag.Duration("task-timeout", 200*time.Millisecond, "per-task context deadline")
	runFor      = flag.Duration("run-for", 5*time.Second, "how long to run the pipeline")
)

// Result holds the outcome of processing one object. Err is non-nil on
// failure or per-task timeout.
type Result struct {
	Key      string
	Duration time.Duration
	Size     int
	Err      error
}

var (
	errCorrupted = errors.New("object corrupted")
	errChecksum  = errors.New("checksum mismatch")
	errQuota     = errors.New("storage quota exceeded")
	taskErrors   = []error{errCorrupted, errChecksum, errQuota}
)

func main() {
	flag.Parse()
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, nil)))

	if *workers <= 0 {
		fmt.Fprintln(os.Stderr, "-workers must be > 0")
		flag.Usage()
		os.Exit(2)
	}
	if *taskTimeout <= 0 {
		fmt.Fprintln(os.Stderr, "-task-timeout must be > 0")
		flag.Usage()
		os.Exit(2)
	}
	if *runFor <= 0 {
		fmt.Fprintln(os.Stderr, "-run-for must be > 0")
		flag.Usage()
		os.Exit(2)
	}

	// Auto-cancel after -run-for; also handle SIGINT.
	timeoutCtx, timeoutCancel := context.WithTimeout(context.Background(), *runFor)
	defer timeoutCancel()
	ctx, stop := signal.NotifyContext(timeoutCtx, os.Interrupt)
	defer stop()

	p := pool.NewWithResults[Result]().
		WithMaxGoroutines(*workers).
		WithTaskTimeout(*taskTimeout)

	slog.Info(
		"pipeline started",
		"workers", *workers,
		"task_timeout", *taskTimeout,
		"run_for", *runFor,
	)

	// Submit objects continuously. GoCtx blocks while all worker slots are
	// full, providing natural backpressure. It returns ctx.Err() the moment
	// the context is cancelled, so the loop exits cleanly.
	store := newStubObjectStore()
	var submitted int
	for i := 0; ; i++ {
		key := fmt.Sprintf("objects/%06d.bin", i)
		idx := i
		if err := p.GoCtx(ctx, func(taskCtx context.Context) (Result, error) {
			return processAndLog(taskCtx, store, key, idx)
		}); err != nil {
			slog.Info("submission stopped", "submitted", submitted, "reason", err)
			break
		}
		submitted++
	}

	// Wait for all in-flight tasks to finish, then show the ordered summary.
	results, _ := p.Wait()
	printSummary(results, submitted)
}

// processAndLog runs the per-object pipeline and logs each completion with its
// submission index. Because tasks complete in a different order than they were
// submitted, the idx values in the live output appear out of sequence — which
// is exactly what the final submission-order summary corrects.
func processAndLog(ctx context.Context, s *stubObjectStore, key string, idx int) (Result, error) {
	r, err := processObject(ctx, s, key)
	r.Err = err
	if err != nil {
		slog.Warn("done", "idx", idx, "key", key, "duration", r.Duration.Round(time.Millisecond), "err", err)
	} else {
		slog.Info("done", "idx", idx, "key", key, "duration", r.Duration.Round(time.Millisecond), "size", r.Size)
	}
	return r, err
}

func processObject(ctx context.Context, s *stubObjectStore, key string) (Result, error) {
	start := time.Now()

	body, err := s.Download(ctx, key)
	if err != nil {
		return Result{Key: key, Duration: time.Since(start)}, fmt.Errorf("download %s: %w", key, err)
	}

	// XOR-scramble as a stand-in for real transformation work.
	out := make([]byte, len(body))
	for i, b := range body {
		out[i] = b ^ 0x42
	}

	if err := s.Upload(ctx, key+".processed", out); err != nil {
		return Result{Key: key, Duration: time.Since(start)}, fmt.Errorf("upload %s: %w", key, err)
	}

	return Result{Key: key, Duration: time.Since(start), Size: len(out)}, nil
}

func printSummary(results []Result, submitted int) {
	var nok, nerr, ntimeout int
	for _, r := range results {
		switch {
		case r.Err == nil:
			nok++
		case errors.Is(r.Err, context.DeadlineExceeded):
			ntimeout++
		default:
			nerr++
		}
	}

	fmt.Fprint(os.Stderr, "\n── Note: live completions above arrived in a different order than submission.\n")
	fmt.Fprint(os.Stderr, "── The table below is the same results reordered by ResultPool into submission order.\n\n")

	fmt.Fprintf(os.Stderr, " %-5s  %-24s  %8s  %s\n", "idx", "key", "duration", "outcome")
	fmt.Fprintf(os.Stderr, " %s  %s  %s  %s\n",
		strings.Repeat("─", 5), strings.Repeat("─", 24),
		strings.Repeat("─", 8), strings.Repeat("─", 40))

	shown := min(summaryRows, len(results))
	for i, r := range results[:shown] {
		outcome := fmt.Sprintf("ok  %dB", r.Size)
		if r.Err != nil {
			outcome = r.Err.Error()
		}
		fmt.Fprintf(os.Stderr, " %-5d  %-24s  %8s  %s\n",
			i, r.Key, r.Duration.Round(time.Millisecond), outcome)
	}
	if len(results) > summaryRows {
		fmt.Fprintf(os.Stderr, " ... (%d of %d shown)\n", summaryRows, len(results))
	}

	fmt.Fprintf(os.Stderr, "\n submitted: %d   ok: %d   errored: %d   timed-out: %d\n\n",
		submitted, nok, nerr, ntimeout)
}

// The stub injects random latency and failures so the example exercises all
// pool code paths without any external dependencies.
type stubObjectStore struct{}

func newStubObjectStore() *stubObjectStore { return &stubObjectStore{} }

func (s *stubObjectStore) Download(ctx context.Context, _ string) ([]byte, error) {
	// Uniform latency 10–300ms. Combined with the default 200ms task timeout,
	// roughly one-third of downloads will be cancelled mid-flight.
	delay := time.Duration(rand.IntN(291)+10) * time.Millisecond
	select {
	case <-time.After(delay):
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	// ~20% failure rate to exercise error aggregation.
	if rand.IntN(5) == 0 {
		return nil, taskErrors[rand.IntN(len(taskErrors))]
	}
	body := make([]byte, 1024)
	for i := range body {
		body[i] = byte(rand.IntN(256))
	}
	return body, nil
}

func (s *stubObjectStore) Upload(ctx context.Context, _ string, _ []byte) error {
	delay := time.Duration(rand.IntN(26)+5) * time.Millisecond // 5–30ms
	select {
	case <-time.After(delay):
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}
