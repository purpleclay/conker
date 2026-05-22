package pool_test

import (
	"context"
	"fmt"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/purpleclay/conker/pool"
)

// benchTasks is the number of no-op tasks submitted per benchmark iteration.
// Large enough to amortise per-Wait overhead; small enough for fast runs.
const benchTasks = 100

// workerCounts returns the concurrency levels exercised by each sub-benchmark:
// fully serial, default (GOMAXPROCS), and over-subscribed (2×GOMAXPROCS).
func workerCounts() []int {
	n := runtime.GOMAXPROCS(0)
	if n == 1 {
		return []int{1, 2}
	}
	return []int{1, n, 2 * n}
}

var noop = func(_ context.Context) error { return nil }

// BenchmarkPool_Go measures the cost of submitting and completing a fixed
// batch of no-op tasks, then resetting, across three concurrency levels.
func BenchmarkPool_Go(b *testing.B) {
	for _, workers := range workerCounts() {
		b.Run(fmt.Sprintf("workers=%d", workers), func(b *testing.B) {
			p := pool.New().WithMaxGoroutines(workers)
			b.ReportAllocs()
			for b.Loop() {
				for range benchTasks {
					p.Go(noop)
				}
				require.NoError(b, p.Wait())
				p.Reset()
			}
		})
	}
}

var noopResult = func(_ context.Context) (struct{}, error) { return struct{}{}, nil }

// BenchmarkResultPool_Go_Ordered measures the overhead of result collection
// and submission-order sorting relative to the base Pool.
func BenchmarkResultPool_Go_Ordered(b *testing.B) {
	for _, workers := range workerCounts() {
		b.Run(fmt.Sprintf("workers=%d", workers), func(b *testing.B) {
			p := pool.NewWithResults[struct{}]().WithMaxGoroutines(workers)
			b.ReportAllocs()
			for b.Loop() {
				for range benchTasks {
					p.Go(noopResult)
				}
				_, err := p.Wait()
				require.NoError(b, err)
				p.Reset()
			}
		})
	}
}

// BenchmarkResultPool_Go_Unordered measures result collection cost without
// the submission-order sort, isolating the sorting overhead by comparison
// with BenchmarkResultPool_Go_Ordered.
func BenchmarkResultPool_Go_Unordered(b *testing.B) {
	for _, workers := range workerCounts() {
		b.Run(fmt.Sprintf("workers=%d", workers), func(b *testing.B) {
			p := pool.NewWithResults[struct{}]().WithMaxGoroutines(workers).WithUnorderedResults()
			b.ReportAllocs()
			for b.Loop() {
				for range benchTasks {
					p.Go(noopResult)
				}
				_, err := p.Wait()
				require.NoError(b, err)
				p.Reset()
			}
		})
	}
}
