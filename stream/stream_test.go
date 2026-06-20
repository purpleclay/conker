package stream_test

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/purpleclay/conker/panics"
	"github.com/purpleclay/conker/stream"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestStream_Go_ExecutesCallback(t *testing.T) {
	s := stream.New()

	var called atomic.Bool
	s.Go(func(_ context.Context) stream.Callback {
		return func() { called.Store(true) }
	})

	s.Wait()
	assert.True(t, called.Load())
}

func TestStream_Go_PreservesSubmissionOrder(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := stream.New()

		var order []int
		// Submit in order but complete in reverse: task 3 finishes first.
		s.Go(func(_ context.Context) stream.Callback {
			time.Sleep(3 * time.Second)
			return func() { order = append(order, 1) }
		})
		s.Go(func(_ context.Context) stream.Callback {
			time.Sleep(2 * time.Second)
			return func() { order = append(order, 2) }
		})
		s.Go(func(_ context.Context) stream.Callback {
			time.Sleep(1 * time.Second)
			return func() { order = append(order, 3) }
		})

		s.Wait()
		assert.Equal(t, []int{1, 2, 3}, order)
	})
}

func TestStream_Go_NilCallbackIsNoOp(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := stream.New()

		var order []int
		s.Go(func(_ context.Context) stream.Callback {
			time.Sleep(2 * time.Second)
			return func() {
				order = append(order, 1)
			}
		})
		s.Go(func(_ context.Context) stream.Callback {
			time.Sleep(1 * time.Second)
			return nil
		}) // no-op
		s.Go(func(_ context.Context) stream.Callback {
			return func() {
				order = append(order, 3)
			}
		})

		s.Wait()
		assert.Equal(t, []int{1, 3}, order)
	})
}

func TestStream_ProducerPanic_PropagatesViaWait(t *testing.T) {
	s := stream.New()
	s.Go(func(_ context.Context) stream.Callback {
		panic("producer went wrong")
	})

	err := func() (r any) {
		defer func() { r = recover() }()
		s.Wait()
		return nil
	}()

	require.NotNil(t, err)
	rec, ok := err.(*panics.Recovered)
	require.True(t, ok, "Wait must re-panic with *panics.Recovered")
	assert.Equal(t, "producer went wrong", rec.Value)
}

func TestStream_CallbackPanic_PropagatesViaWait(t *testing.T) {
	s := stream.New()
	s.Go(func(_ context.Context) stream.Callback {
		return func() { panic("callback went wrong") }
	})

	err := func() (r any) {
		defer func() { r = recover() }()
		s.Wait()
		return nil
	}()

	require.NotNil(t, err)
	rec, ok := err.(*panics.Recovered)
	require.True(t, ok, "Wait must re-panic with *panics.Recovered")
	assert.Equal(t, "callback went wrong", rec.Value)
}

func TestStream_ProducerPanic_DoesNotDeadlockLateSubmitters(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := stream.New().WithMaxGoroutines(1)

		// First producer panics; the dispatcher must keep draining submitted
		// afterwards or these later Go calls block forever once the buffer
		// (sized cap(sem)) fills up with undrained slots.
		s.Go(func(_ context.Context) stream.Callback {
			panic("producer went wrong")
		})
		s.Go(func(_ context.Context) stream.Callback { return nil })
		s.Go(func(_ context.Context) stream.Callback { return nil })

		assert.Panics(t, s.Wait)
	})
}

func TestStream_CallbackPanic_DoesNotDeadlockLateSubmitters(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := stream.New().WithMaxGoroutines(1)

		s.Go(func(_ context.Context) stream.Callback {
			return func() { panic("callback went wrong") }
		})
		s.Go(func(_ context.Context) stream.Callback { return nil })
		s.Go(func(_ context.Context) stream.Callback { return nil })

		assert.Panics(t, s.Wait)
	})
}

func TestStream_ProducerPanicDuringDrain_OutranksEarlierCallbackPanic(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := stream.New().WithMaxGoroutines(1)

		// The callback panic dispatches first; the producer panic is only
		// discovered afterwards, while the dispatcher drains the remaining
		// slot. Producer panics must still win — they preserve the original
		// panic site's stack trace, unlike a re-wrapped callback panic.
		s.Go(func(_ context.Context) stream.Callback {
			return func() { panic("callback went wrong") }
		})
		s.Go(func(_ context.Context) stream.Callback {
			panic("producer went wrong")
		})

		err := func() (r any) {
			defer func() { r = recover() }()
			s.Wait()
			return nil
		}()

		require.NotNil(t, err)
		rec, ok := err.(*panics.Recovered)
		require.True(t, ok, "Wait must re-panic with *panics.Recovered")
		assert.Equal(t, "producer went wrong", rec.Value, "producer panic must win even when discovered during post-panic drain")
	})
}

func TestStream_Wait_EmptyIsNoOp(t *testing.T) {
	s := stream.New()
	require.NotPanics(t, s.Wait)
}

func TestStream_Wait_IdempotentAfterTasks(t *testing.T) {
	s := stream.New()
	s.Go(func(_ context.Context) stream.Callback { return nil })

	s.Wait()
	require.NotPanics(t, s.Wait, "second Wait must be a no-op, not a close-of-closed-channel panic")
}

func TestStream_Wait_IdempotentWithNoTasks(t *testing.T) {
	s := stream.New()
	s.Wait()
	require.NotPanics(t, s.Wait)
}

func TestStream_WithMaxGoroutines_PanicsOnInvalidN(t *testing.T) {
	assert.Panics(t, func() { stream.New().WithMaxGoroutines(0) })
	assert.Panics(t, func() { stream.New().WithMaxGoroutines(-1) })
}

func TestStream_WithMaxGoroutines_PanicsAfterGo(t *testing.T) {
	s := stream.New()
	s.Go(func(_ context.Context) stream.Callback { return nil })

	assert.Panics(t, func() { s.WithMaxGoroutines(4) })
	s.Wait()
}

func TestStream_DefaultConcurrency(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := stream.New()

		var concurrent, peak atomic.Int64
		for range 4 {
			s.Go(func(_ context.Context) stream.Callback {
				n := concurrent.Add(1)
				for {
					if cur := peak.Load(); n <= cur || peak.CompareAndSwap(cur, n) {
						break
					}
				}
				time.Sleep(100 * time.Millisecond)
				concurrent.Add(-1)
				return nil
			})
		}

		s.Wait()
		if runtime.GOMAXPROCS(0) == 1 {
			assert.Equal(t, int64(1), peak.Load(), "with GOMAXPROCS=1 only one producer can run at once")
			return
		}
		assert.Greater(t, peak.Load(), int64(1), "default stream must run producers concurrently")
	})
}

func TestStream_GoCtx_ReturnsCancelledWhenBlocked(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := stream.New().WithMaxGoroutines(1)

		// Fill the only slot with a slow producer.
		s.Go(func(_ context.Context) stream.Callback {
			time.Sleep(time.Second)
			return nil
		})

		// Attempt submission with a context that times out before the slot frees.
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()

		var taskRan atomic.Bool
		err := s.GoCtx(ctx, func(_ context.Context) stream.Callback {
			taskRan.Store(true)
			return nil
		})

		assert.ErrorIs(t, err, context.DeadlineExceeded)
		assert.False(t, taskRan.Load(), "producer must not run when GoCtx returns an error")

		s.Wait()
	})
}

func TestStream_Go_AfterWait_PanicsWithClearMessage(t *testing.T) {
	s := stream.New()
	s.Go(func(_ context.Context) stream.Callback { return nil })
	s.Wait()

	assert.PanicsWithValue(t, "stream: Go called after Wait — Stream is single-shot", func() {
		s.Go(func(_ context.Context) stream.Callback { return nil })
	})
}

func TestStream_GoCtx_AfterWait_PanicsWithClearMessage(t *testing.T) {
	s := stream.New()
	s.Go(func(_ context.Context) stream.Callback { return nil })
	s.Wait()

	assert.PanicsWithValue(t, "stream: Go called after Wait — Stream is single-shot", func() {
		_ = s.GoCtx(context.Background(), func(_ context.Context) stream.Callback { return nil })
	})
}

func TestStream_Go_RacingWithWait_NeverPanicsOnClosedChannel(t *testing.T) {
	const racers = 10

	for range 200 {
		s := stream.New().WithMaxGoroutines(4)

		var wg sync.WaitGroup
		for range racers {
			wg.Go(func() {
				defer func() {
					if r := recover(); r != nil {
						// The only acceptable panic is our own clear message —
						// never the raw "send on closed channel" runtime error.
						assert.Equal(t, "stream: Go called after Wait — Stream is single-shot", r)
					}
				}()
				s.Go(func(_ context.Context) stream.Callback { return nil })
			})
		}

		s.Wait()
		wg.Wait()
	}
}

func TestStream_Producer_ReceivesStreamContext(t *testing.T) {
	s := stream.New()

	var producerCtx context.Context
	s.Go(func(ctx context.Context) stream.Callback {
		producerCtx = ctx
		return nil
	})

	s.Wait()
	// After Wait, the stream's internal context is cancelled.
	assert.ErrorIs(t, producerCtx.Err(), context.Canceled)
}
