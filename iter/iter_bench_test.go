package iter_test

import (
	"fmt"
	stditer "iter"
	"runtime"
	"slices"
	"testing"

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
