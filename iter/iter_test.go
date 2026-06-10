package iter_test

import (
	"context"
	"errors"
	"fmt"
	stditer "iter"
	"slices"
	"strconv"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	conkiter "github.com/purpleclay/conker/iter"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestMapSeq_TransformsElements(t *testing.T) {
	in := slices.Values([]int{1, 2, 3, 4, 5})
	out := slices.Collect(conkiter.MapSeq(in, func(v int) int { return v * 2 }))
	assert.Equal(t, []int{2, 4, 6, 8, 10}, out)
}

func TestMapSeq_EmptySeq(t *testing.T) {
	in := slices.Values([]int{})
	out := slices.Collect(conkiter.MapSeq(in, func(v int) int { return v }))
	assert.Empty(t, out)
}

func TestMapSeq_PreservesSubmissionOrder(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		delays := []time.Duration{
			3 * time.Second,
			1 * time.Second,
			2 * time.Second,
		}
		in := slices.Values(delays)

		out := slices.Collect(conkiter.MapSeq(in, func(d time.Duration) time.Duration {
			time.Sleep(d)
			return d
		}))

		// Results must match submission order regardless of completion order.
		assert.Equal(t, delays, out)
	})
}

func TestMapSeq_EarlyBreakStopsProcessing(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const (
			breakAfter = 3
			maxWorkers = 1
		)

		var processed atomic.Int64
		in := slices.Values([]int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10})

		var collected []int
		for v := range conkiter.MapSeq(in, func(v int) int {
			processed.Add(1)
			return v
		}, conkiter.WithMaxGoroutines(maxWorkers)) {
			collected = append(collected, v)
			if len(collected) == breakAfter {
				break
			}
		}

		// Drain any goroutine that was in-flight at break time before asserting.
		synctest.Wait()

		assert.Equal(t, []int{1, 2, 3}, collected)
		// With maxWorkers=1, at most one goroutine beyond breakAfter could have
		// been dispatched before the done signal propagated.
		assert.LessOrEqual(t, processed.Load(), int64(breakAfter+maxWorkers),
			"must not process more than breakAfter+maxWorkers elements after early break")
	})
}

func TestMapSeq_WithMaxGoroutines_LimitsConcurrency(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var concurrent, peak atomic.Int64

		in := slices.Values([]int{1, 2, 3, 4, 5, 6, 7, 8})
		_ = slices.Collect(conkiter.MapSeq(in, func(v int) int {
			n := concurrent.Add(1)
			for {
				if cur := peak.Load(); n <= cur || peak.CompareAndSwap(cur, n) {
					break
				}
			}
			time.Sleep(100 * time.Millisecond)
			concurrent.Add(-1)
			return v
		}, conkiter.WithMaxGoroutines(3)))

		assert.LessOrEqual(t, peak.Load(), int64(3))
	})
}

func TestMapSeq_WithContext_StopsOnCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already cancelled

	var processed atomic.Int64
	in := slices.Values([]int{1, 2, 3, 4, 5})
	_ = slices.Collect(conkiter.MapSeq(in, func(v int) int {
		processed.Add(1)
		return v
	}, conkiter.WithContext(ctx), conkiter.WithMaxGoroutines(1)))

	assert.Equal(t, int64(0), processed.Load(), "pre-cancelled context must dispatch zero elements")
}

func TestMapSeq_WithContext_CancelMidStream(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const maxWorkers = 3

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// Cancel the context after tasks are in-flight but before they finish.
		go func() {
			time.Sleep(500 * time.Millisecond)
			cancel()
		}()

		var processed atomic.Int64
		in := slices.Values(make([]int, 20))
		_ = slices.Collect(conkiter.MapSeq(in, func(v int) int {
			processed.Add(1)
			time.Sleep(time.Second)
			return v
		}, conkiter.WithContext(ctx), conkiter.WithMaxGoroutines(maxWorkers)))

		// slices.Collect waits for each in-flight slot to complete before
		// returning, so processed is fully settled — no synctest.Wait needed.
		assert.LessOrEqual(t, processed.Load(), int64(maxWorkers),
			"cancellation must not dispatch beyond the in-flight goroutines at cancel time")
	})
}

func TestMapSeq2_TransformsKeyValuePairs(t *testing.T) {
	in := func(yield func(string, int) bool) {
		for _, p := range []struct {
			k string
			v int
		}{{"a", 1}, {"b", 2}, {"c", 3}} {
			if !yield(p.k, p.v) {
				return
			}
		}
	}

	out := slices.Collect(conkiter.MapSeq2(stditer.Seq2[string, int](in), func(k string, v int) string {
		return k + ":" + strconv.Itoa(v)
	}))

	assert.Len(t, out, 3)
	assert.Contains(t, out, "a:1")
	assert.Contains(t, out, "b:2")
	assert.Contains(t, out, "c:3")
}

func TestMapSeq2_PreservesSubmissionOrder(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		in := func(yield func(int, time.Duration) bool) {
			pairs := []struct {
				idx   int
				delay time.Duration
			}{
				{0, 3 * time.Second},
				{1, 1 * time.Second},
				{2, 2 * time.Second},
			}
			for _, p := range pairs {
				if !yield(p.idx, p.delay) {
					return
				}
			}
		}

		out := slices.Collect(conkiter.MapSeq2(stditer.Seq2[int, time.Duration](in), func(idx int, d time.Duration) int {
			time.Sleep(d)
			return idx
		}))

		assert.Equal(t, []int{0, 1, 2}, out)
	})
}

func TestForEachSeq_ProcessesAllElements(t *testing.T) {
	var count atomic.Int64
	in := slices.Values([]int{1, 2, 3, 4, 5})
	conkiter.ForEachSeq(in, func(_ int) { count.Add(1) })
	assert.Equal(t, int64(5), count.Load())
}

func TestForEachSeq_WithContext_StopsOnCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	var processed atomic.Int64
	in := slices.Values([]int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10})
	conkiter.ForEachSeq(in, func(_ int) {
		processed.Add(1)
	}, conkiter.WithContext(ctx), conkiter.WithMaxGoroutines(1))

	assert.Equal(t, int64(0), processed.Load(), "pre-cancelled context must dispatch zero elements")
}

func TestForEachSeq_WithContext_CancelMidStream(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const maxWorkers = 3

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// Cancel the context after tasks are in-flight but before they finish.
		go func() {
			time.Sleep(500 * time.Millisecond)
			cancel()
		}()

		var processed atomic.Int64
		in := slices.Values(make([]int, 20))
		conkiter.ForEachSeq(in, func(_ int) {
			processed.Add(1)
			time.Sleep(time.Second)
		}, conkiter.WithContext(ctx), conkiter.WithMaxGoroutines(maxWorkers))

		// ForEachSeq blocks on wg.Wait() until all in-flight goroutines finish,
		// so processed is fully settled when we assert.
		assert.LessOrEqual(t, processed.Load(), int64(maxWorkers),
			"cancellation must not dispatch beyond the in-flight goroutines at cancel time")
	})
}

func TestForEachSeq_WithMaxGoroutines_LimitsConcurrency(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var concurrent, peak atomic.Int64
		in := slices.Values([]int{1, 2, 3, 4, 5, 6})

		conkiter.ForEachSeq(in, func(_ int) {
			n := concurrent.Add(1)
			for {
				if cur := peak.Load(); n <= cur || peak.CompareAndSwap(cur, n) {
					break
				}
			}
			time.Sleep(100 * time.Millisecond)
			concurrent.Add(-1)
		}, conkiter.WithMaxGoroutines(2))

		assert.LessOrEqual(t, peak.Load(), int64(2))
	})
}

func TestMapMap_ReturnsResultForEveryPair(t *testing.T) {
	in := map[string]int{"a": 1, "b": 2, "c": 3}
	out := conkiter.MapMap(in, func(k string, v int) string { return k + ":" + strconv.Itoa(v) })
	assert.ElementsMatch(t, []string{"a:1", "b:2", "c:3"}, out)
}

func TestMapMap_EmptyMap(t *testing.T) {
	out := conkiter.MapMap(map[string]int{}, func(k string, _ int) string { return k })
	assert.Empty(t, out)
}

func TestMapMap_WithMaxGoroutines_LimitsConcurrency(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var concurrent, peak atomic.Int64
		in := map[int]struct{}{0: {}, 1: {}, 2: {}, 3: {}, 4: {}, 5: {}, 6: {}, 7: {}}

		_ = conkiter.MapMap(in, func(k int, _ struct{}) int {
			n := concurrent.Add(1)
			for {
				if cur := peak.Load(); n <= cur || peak.CompareAndSwap(cur, n) {
					break
				}
			}
			time.Sleep(100 * time.Millisecond)
			concurrent.Add(-1)
			return k
		}, conkiter.WithMaxGoroutines(3))

		assert.LessOrEqual(t, peak.Load(), int64(3))
	})
}

func TestMapMap_WithContext_StopsOnCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	var processed atomic.Int64
	in := map[int]struct{}{0: {}, 1: {}, 2: {}, 3: {}, 4: {}}
	_ = conkiter.MapMap(in, func(k int, _ struct{}) int {
		processed.Add(1)
		return k
	}, conkiter.WithContext(ctx), conkiter.WithMaxGoroutines(1))

	assert.Equal(t, int64(0), processed.Load(), "pre-cancelled context must dispatch zero elements")
}

func TestForEachMap_ProcessesAllPairs(t *testing.T) {
	var count atomic.Int64
	in := map[string]int{"a": 1, "b": 2, "c": 3}
	conkiter.ForEachMap(in, func(_ string, _ int) { count.Add(1) })
	assert.Equal(t, int64(3), count.Load())
}

func TestForEachMap_EmptyMap(t *testing.T) {
	var count atomic.Int64
	conkiter.ForEachMap(map[string]int{}, func(_ string, _ int) { count.Add(1) })
	assert.Equal(t, int64(0), count.Load())
}

func TestForEachMap_WithMaxGoroutines_LimitsConcurrency(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var concurrent, peak atomic.Int64
		in := map[int]struct{}{0: {}, 1: {}, 2: {}, 3: {}, 4: {}, 5: {}}

		conkiter.ForEachMap(in, func(_ int, _ struct{}) {
			n := concurrent.Add(1)
			for {
				if cur := peak.Load(); n <= cur || peak.CompareAndSwap(cur, n) {
					break
				}
			}
			time.Sleep(100 * time.Millisecond)
			concurrent.Add(-1)
		}, conkiter.WithMaxGoroutines(2))

		assert.LessOrEqual(t, peak.Load(), int64(2))
	})
}

func TestForEachMap_WithContext_StopsOnCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	var processed atomic.Int64
	in := map[int]struct{}{0: {}, 1: {}, 2: {}, 3: {}, 4: {}, 5: {}, 6: {}, 7: {}, 8: {}, 9: {}}
	conkiter.ForEachMap(in, func(_ int, _ struct{}) {
		processed.Add(1)
	}, conkiter.WithContext(ctx), conkiter.WithMaxGoroutines(1))

	assert.Equal(t, int64(0), processed.Load(), "pre-cancelled context must dispatch zero elements")
}

func TestMapSeqErr_TransformsElements(t *testing.T) {
	in := slices.Values([]int{1, 2, 3, 4, 5})
	out, err := conkiter.MapSeqErr(in, func(_ context.Context, v int) (int, error) { return v * 2, nil })
	require.NoError(t, err)
	assert.Equal(t, []int{2, 4, 6, 8, 10}, out)
}

func TestMapSeqErr_EmptySeq(t *testing.T) {
	out, err := conkiter.MapSeqErr(slices.Values([]int{}), func(_ context.Context, v int) (int, error) { return v, nil })
	require.NoError(t, err)
	assert.Empty(t, out)
}

func TestMapSeqErr_CollectsErrorsAndResults(t *testing.T) {
	in := slices.Values([]int{1, 2, 3, 4, 5})
	out, err := conkiter.MapSeqErr(in, func(_ context.Context, v int) (int, error) {
		if v%2 == 0 {
			return 99, fmt.Errorf("even: %d", v) // non-zero return alongside error
		}
		return v * 2, nil
	}, conkiter.WithMaxGoroutines(1))

	require.Error(t, err)
	assert.ErrorContains(t, err, "even: 2")
	assert.ErrorContains(t, err, "even: 4")
	assert.Equal(t, []int{2, 0, 6, 0, 10}, out, "all slots collected; errored elements should have zero values")
}

func TestMapSeqErr_PreservesSubmissionOrder(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		delays := []time.Duration{3 * time.Second, 1 * time.Second, 2 * time.Second}
		out, err := conkiter.MapSeqErr(slices.Values(delays), func(_ context.Context, d time.Duration) (time.Duration, error) {
			time.Sleep(d)
			return d, nil
		})
		require.NoError(t, err)
		assert.Equal(t, delays, out)
	})
}

func TestMapSeqErr_GoverningContextCancelsInflightFn(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())

		go func() {
			time.Sleep(100 * time.Millisecond)
			cancel()
		}()

		var sawCancel atomic.Bool
		in := slices.Values([]int{1})
		_, err := conkiter.MapSeqErr(in, func(fnCtx context.Context, _ int) (int, error) {
			<-fnCtx.Done()
			sawCancel.Store(true)
			return 0, fnCtx.Err()
		}, conkiter.WithContext(ctx))

		require.Error(t, err)
		assert.ErrorIs(t, err, context.Canceled, "error should be context.Canceled from governing context")
		assert.True(t, sawCancel.Load(), "fn must observe governing context cancellation")
	})
}

func TestMapSeqErr_WithCancelOnError_CancelsInflightWork(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var sawCancel atomic.Bool

		in := slices.Values([]int{1, 2})
		_, err := conkiter.MapSeqErr(in, func(ctx context.Context, v int) (int, error) {
			if v == 1 {
				return 0, errors.New("deliberate error")
			}
			<-ctx.Done()
			sawCancel.Store(true)
			return 0, nil
		}, conkiter.WithMaxGoroutines(2), conkiter.WithCancelOnError())

		require.Error(t, err)
		assert.True(t, sawCancel.Load(), "in-flight work must observe cancellation when a peer errors")
	})
}

func TestForEachSeqErr_ProcessesAllElements(t *testing.T) {
	var count atomic.Int64
	in := slices.Values([]int{1, 2, 3, 4, 5})
	err := conkiter.ForEachSeqErr(in, func(_ context.Context, _ int) error {
		count.Add(1)
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, int64(5), count.Load())
}

func TestForEachSeqErr_EmptySeq(t *testing.T) {
	err := conkiter.ForEachSeqErr(slices.Values([]int{}), func(_ context.Context, _ int) error { return nil })
	assert.NoError(t, err)
}

func TestForEachSeqErr_CollectsAllErrors(t *testing.T) {
	in := slices.Values([]int{1, 2, 3})
	err := conkiter.ForEachSeqErr(in, func(_ context.Context, v int) error {
		return fmt.Errorf("error %d", v)
	}, conkiter.WithMaxGoroutines(3))

	require.Error(t, err)
	assert.ErrorContains(t, err, "error 1")
	assert.ErrorContains(t, err, "error 2")
	assert.ErrorContains(t, err, "error 3")
}

func TestForEachSeqErr_GoverningContextCancelsInflightFn(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())

		go func() {
			time.Sleep(100 * time.Millisecond)
			cancel()
		}()

		var sawCancel atomic.Bool
		in := slices.Values([]int{1})
		err := conkiter.ForEachSeqErr(in, func(fnCtx context.Context, _ int) error {
			<-fnCtx.Done()
			sawCancel.Store(true)
			return fnCtx.Err()
		}, conkiter.WithContext(ctx))

		require.Error(t, err)
		assert.ErrorIs(t, err, context.Canceled, "error should be context.Canceled from governing context")
		assert.True(t, sawCancel.Load(), "fn must observe governing context cancellation")
	})
}

func TestForEachSeqErr_WithCancelOnError_CancelsInflightWork(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var sawCancel atomic.Bool

		in := slices.Values([]int{1, 2})
		err := conkiter.ForEachSeqErr(in, func(ctx context.Context, v int) error {
			if v == 1 {
				return errors.New("deliberate error")
			}
			<-ctx.Done()
			sawCancel.Store(true)
			return nil
		}, conkiter.WithMaxGoroutines(2), conkiter.WithCancelOnError())

		require.Error(t, err)
		assert.True(t, sawCancel.Load(), "in-flight work must observe cancellation when a peer errors")
	})
}

func TestWithCancelOnError_LimitsDispatchAfterError(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const maxWorkers = 2
		var dispatched atomic.Int64

		in := slices.Values(make([]int, 20))
		_ = conkiter.ForEachSeqErr(in, func(_ context.Context, _ int) error {
			dispatched.Add(1)
			return errors.New("error")
		}, conkiter.WithMaxGoroutines(maxWorkers), conkiter.WithCancelOnError())

		assert.LessOrEqual(t, dispatched.Load(), int64(maxWorkers),
			"WithCancelOnError must stop dispatch beyond the initial in-flight batch")
	})
}

func TestWithMaxGoroutines_PanicsOnInvalidN(t *testing.T) {
	require.Panics(t, func() { conkiter.WithMaxGoroutines(0) })
	require.Panics(t, func() { conkiter.WithMaxGoroutines(-1) })
}

func TestWithContext_PanicsOnNilContext(t *testing.T) {
	require.Panics(t, func() { conkiter.WithContext(nil) })
}
