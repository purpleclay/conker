package stream_test

import (
	"context"
	"fmt"
	"runtime"
	"testing"

	"github.com/purpleclay/conker/stream"
)

const benchTasks = 100

func workerCounts() []int {
	n := runtime.GOMAXPROCS(0)
	if n == 1 {
		return []int{1, 2}
	}
	return []int{1, n, 2 * n}
}

var noopProducer = func(_ context.Context) stream.Callback { return nil }

// BenchmarkStream_Go measures the cost of submitting and completing a fixed
// batch of no-op producers across three concurrency levels. Neither the
// producer nor the callback performs any work, isolating stream machinery
// overhead: semaphore, slot channel, dispatcher, and callback dispatch.
func BenchmarkStream_Go(b *testing.B) {
	for _, workers := range workerCounts() {
		b.Run(fmt.Sprintf("workers=%d", workers), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				s := stream.New().WithMaxGoroutines(workers)
				for range benchTasks {
					s.Go(noopProducer)
				}
				s.Wait()
			}
		})
	}
}
