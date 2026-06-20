package iter_test

import (
	"context"
	"fmt"
	stditer "iter"
	"runtime"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	conkiter "github.com/purpleclay/conker/iter"
)

const benchTasks = 100

func workerCounts() []int {
	n := runtime.GOMAXPROCS(0)
	if n == 1 {
		return []int{1, 2}
	}
	return []int{1, n, 2 * n}
}

// BenchmarkMapSeq measures the ordered concurrent mapping machinery: semaphore,
// ordered channel, and goroutine coordination, using a no-op mapping function
// across three concurrency levels.
func BenchmarkMapSeq(b *testing.B) {
	in := slices.Values(make([]struct{}, benchTasks))
	for _, workers := range workerCounts() {
		b.Run(fmt.Sprintf("workers=%d", workers), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				_ = slices.Collect(conkiter.MapSeq(in, func(v struct{}) struct{} { return v }, conkiter.WithMaxGoroutines(workers)))
			}
		})
	}
}

// BenchmarkMapSeq2 measures the additional pair-wrapping overhead of MapSeq2
// relative to MapSeq across three concurrency levels.
func BenchmarkMapSeq2(b *testing.B) {
	pairs := stditer.Seq2[int, struct{}](func(yield func(int, struct{}) bool) {
		for i := range benchTasks {
			if !yield(i, struct{}{}) {
				return
			}
		}
	})
	for _, workers := range workerCounts() {
		b.Run(fmt.Sprintf("workers=%d", workers), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				_ = slices.Collect(conkiter.MapSeq2(pairs, func(_ int, v struct{}) struct{} { return v }, conkiter.WithMaxGoroutines(workers)))
			}
		})
	}
}

// BenchmarkMapSeqErr measures the overhead MapSeqErr adds over MapSeq on the
// all-success path: a derived, cancellable context per call and the
// mutex-guarded error slice, even though no errors are ever collected.
func BenchmarkMapSeqErr(b *testing.B) {
	in := slices.Values(make([]struct{}, benchTasks))
	for _, workers := range workerCounts() {
		b.Run(fmt.Sprintf("workers=%d", workers), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				_, err := conkiter.MapSeqErr(in, func(_ context.Context, v struct{}) (struct{}, error) { return v, nil }, conkiter.WithMaxGoroutines(workers))
				require.NoError(b, err)
			}
		})
	}
}

// BenchmarkForEachSeq measures concurrent iteration overhead: semaphore and
// goroutine coordination with a no-op function, across three concurrency levels.
func BenchmarkForEachSeq(b *testing.B) {
	in := slices.Values(make([]struct{}, benchTasks))
	for _, workers := range workerCounts() {
		b.Run(fmt.Sprintf("workers=%d", workers), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				conkiter.ForEachSeq(in, func(_ struct{}) {}, conkiter.WithMaxGoroutines(workers))
			}
		})
	}
}

// BenchmarkForEachSeqErr measures the overhead ForEachSeqErr adds over
// ForEachSeq on the all-success path: a derived, cancellable context per call
// and the mutex-guarded error slice, even though no errors are ever collected.
func BenchmarkForEachSeqErr(b *testing.B) {
	in := slices.Values(make([]struct{}, benchTasks))
	for _, workers := range workerCounts() {
		b.Run(fmt.Sprintf("workers=%d", workers), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				err := conkiter.ForEachSeqErr(in, func(_ context.Context, _ struct{}) error { return nil }, conkiter.WithMaxGoroutines(workers))
				require.NoError(b, err)
			}
		})
	}
}

// BenchmarkMapMap measures the overhead of MapMap relative to MapSeq2: the
// maps.All adapter plus slices.Collect, across three concurrency levels.
func BenchmarkMapMap(b *testing.B) {
	in := make(map[int]struct{}, benchTasks)
	for i := range benchTasks {
		in[i] = struct{}{}
	}
	for _, workers := range workerCounts() {
		b.Run(fmt.Sprintf("workers=%d", workers), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				_ = conkiter.MapMap(in, func(_ int, v struct{}) struct{} { return v }, conkiter.WithMaxGoroutines(workers))
			}
		})
	}
}

// BenchmarkForEachMap measures the concurrent foreach overhead over a Go map
// across three concurrency levels.
func BenchmarkForEachMap(b *testing.B) {
	in := make(map[int]struct{}, benchTasks)
	for i := range benchTasks {
		in[i] = struct{}{}
	}
	for _, workers := range workerCounts() {
		b.Run(fmt.Sprintf("workers=%d", workers), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				conkiter.ForEachMap(in, func(_ int, _ struct{}) {}, conkiter.WithMaxGoroutines(workers))
			}
		})
	}
}
