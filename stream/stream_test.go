package stream_test

import (
	"context"
	"errors"
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
	s.Go(func(_ context.Context) (stream.Callback, error) {
		return func() { called.Store(true) }, nil
	})

	s.Wait()
	assert.True(t, called.Load())
}

func TestStream_Go_PreservesSubmissionOrder(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := stream.New()

		var order []int
		// Submit in order but complete in reverse: task 3 finishes first.
		s.Go(func(_ context.Context) (stream.Callback, error) {
			time.Sleep(3 * time.Second)
			return func() { order = append(order, 1) }, nil
		})
		s.Go(func(_ context.Context) (stream.Callback, error) {
			time.Sleep(2 * time.Second)
			return func() { order = append(order, 2) }, nil
		})
		s.Go(func(_ context.Context) (stream.Callback, error) {
			time.Sleep(1 * time.Second)
			return func() { order = append(order, 3) }, nil
		})

		s.Wait()
		assert.Equal(t, []int{1, 2, 3}, order)
	})
}

func TestStream_Go_NilCallbackIsNoOp(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := stream.New()

		var order []int
		s.Go(func(_ context.Context) (stream.Callback, error) {
			time.Sleep(2 * time.Second)
			return func() {
				order = append(order, 1)
			}, nil
		})
		s.Go(func(_ context.Context) (stream.Callback, error) {
			time.Sleep(1 * time.Second)
			return nil, nil
		}) // no-op
		s.Go(func(_ context.Context) (stream.Callback, error) {
			return func() {
				order = append(order, 3)
			}, nil
		})

		s.Wait()
		assert.Equal(t, []int{1, 3}, order)
	})
}

func TestStream_ProducerPanic_PropagatesViaWait(t *testing.T) {
	s := stream.New()
	s.Go(func(_ context.Context) (stream.Callback, error) {
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
	s.Go(func(_ context.Context) (stream.Callback, error) {
		return func() { panic("callback went wrong") }, nil
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
		s.Go(func(_ context.Context) (stream.Callback, error) {
			panic("producer went wrong")
		})
		s.Go(func(_ context.Context) (stream.Callback, error) { return nil, nil })
		s.Go(func(_ context.Context) (stream.Callback, error) { return nil, nil })

		assert.Panics(t, func() { _ = s.Wait() })
	})
}

func TestStream_CallbackPanic_DoesNotDeadlockLateSubmitters(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := stream.New().WithMaxGoroutines(1)

		s.Go(func(_ context.Context) (stream.Callback, error) {
			return func() { panic("callback went wrong") }, nil
		})
		s.Go(func(_ context.Context) (stream.Callback, error) { return nil, nil })
		s.Go(func(_ context.Context) (stream.Callback, error) { return nil, nil })

		assert.Panics(t, func() { _ = s.Wait() })
	})
}

func TestStream_ProducerPanicDuringDrain_OutranksEarlierCallbackPanic(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := stream.New().WithMaxGoroutines(1)

		// The callback panic dispatches first; the producer panic is only
		// discovered afterwards, while the dispatcher drains the remaining
		// slot. Producer panics must still win — they preserve the original
		// panic site's stack trace, unlike a re-wrapped callback panic.
		s.Go(func(_ context.Context) (stream.Callback, error) {
			return func() { panic("callback went wrong") }, nil
		})
		s.Go(func(_ context.Context) (stream.Callback, error) {
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
	require.NotPanics(t, func() { _ = s.Wait() })
}

func TestStream_Wait_IdempotentAfterTasks(t *testing.T) {
	s := stream.New()
	s.Go(func(_ context.Context) (stream.Callback, error) { return nil, nil })

	s.Wait()
	require.NotPanics(t, func() { _ = s.Wait() }, "second Wait must be a no-op, not a close-of-closed-channel panic")
}

func TestStream_Wait_IdempotentWithNoTasks(t *testing.T) {
	s := stream.New()
	s.Wait()
	require.NotPanics(t, func() { _ = s.Wait() })
}

func TestStream_WithMaxGoroutines_PanicsOnInvalidN(t *testing.T) {
	assert.Panics(t, func() { stream.New().WithMaxGoroutines(0) })
	assert.Panics(t, func() { stream.New().WithMaxGoroutines(-1) })
}

func TestStream_WithMaxGoroutines_PanicsAfterGo(t *testing.T) {
	s := stream.New()
	s.Go(func(_ context.Context) (stream.Callback, error) { return nil, nil })

	assert.Panics(t, func() { s.WithMaxGoroutines(4) })
	s.Wait()
}

func TestStream_DefaultConcurrency(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := stream.New()

		var concurrent, peak atomic.Int64
		for range 4 {
			s.Go(func(_ context.Context) (stream.Callback, error) {
				n := concurrent.Add(1)
				for {
					if cur := peak.Load(); n <= cur || peak.CompareAndSwap(cur, n) {
						break
					}
				}
				time.Sleep(100 * time.Millisecond)
				concurrent.Add(-1)
				return nil, nil
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
		s.Go(func(_ context.Context) (stream.Callback, error) {
			time.Sleep(time.Second)
			return nil, nil
		})

		// Attempt submission with a context that times out before the slot frees.
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()

		var taskRan atomic.Bool
		err := s.GoCtx(ctx, func(_ context.Context) (stream.Callback, error) {
			taskRan.Store(true)
			return nil, nil
		})

		assert.ErrorIs(t, err, context.DeadlineExceeded)
		assert.False(t, taskRan.Load(), "producer must not run when GoCtx returns an error")

		s.Wait()
	})
}

func TestStream_Go_AfterWait_PanicsWithClearMessage(t *testing.T) {
	s := stream.New()
	s.Go(func(_ context.Context) (stream.Callback, error) { return nil, nil })
	s.Wait()

	assert.PanicsWithValue(t, "stream: Go called after Wait — call Reset to reuse the Stream", func() {
		s.Go(func(_ context.Context) (stream.Callback, error) { return nil, nil })
	})
}

func TestStream_GoCtx_AfterWait_PanicsWithClearMessage(t *testing.T) {
	s := stream.New()
	s.Go(func(_ context.Context) (stream.Callback, error) { return nil, nil })
	s.Wait()

	assert.PanicsWithValue(t, "stream: Go called after Wait — call Reset to reuse the Stream", func() {
		_ = s.GoCtx(context.Background(), func(_ context.Context) (stream.Callback, error) { return nil, nil })
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
						assert.Equal(t, "stream: Go called after Wait — call Reset to reuse the Stream", r)
					}
				}()
				s.Go(func(_ context.Context) (stream.Callback, error) { return nil, nil })
			})
		}

		s.Wait()
		wg.Wait()
	}
}

func TestStream_Producer_ReceivesStreamContext(t *testing.T) {
	s := stream.New()

	var producerCtx context.Context
	s.Go(func(ctx context.Context) (stream.Callback, error) {
		producerCtx = ctx
		return nil, nil
	})

	s.Wait()
	// After Wait, the stream's internal context is cancelled.
	assert.ErrorIs(t, producerCtx.Err(), context.Canceled)
}

func TestStream_Wait_NilWhenNoProducerErrors(t *testing.T) {
	s := stream.New()
	s.Go(func(_ context.Context) (stream.Callback, error) { return nil, nil })

	require.NoError(t, s.Wait())
}

func TestStream_ProducerErrors_JoinedInSubmissionOrder(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := stream.New()
		first := errors.New("first")
		second := errors.New("second")

		// The later submission fails first; Wait must still report submission order.
		s.Go(func(_ context.Context) (stream.Callback, error) {
			time.Sleep(2 * time.Second)
			return nil, first
		})
		s.Go(func(_ context.Context) (stream.Callback, error) {
			time.Sleep(time.Second)
			return nil, second
		})

		err := s.Wait()
		require.Error(t, err)
		joined, ok := err.(interface{ Unwrap() []error })
		require.True(t, ok, "Wait must return errors.Join of producer errors")
		assert.Equal(t, []error{first, second}, joined.Unwrap())
	})
}

func TestStream_ProducerError_SkipsItsCallbackButLaterCallbacksRunInOrder(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := stream.New()
		sentinel := errors.New("producer failed")

		var order []int
		s.Go(func(_ context.Context) (stream.Callback, error) {
			time.Sleep(3 * time.Second)
			return func() { order = append(order, 1) }, nil
		})
		s.Go(func(_ context.Context) (stream.Callback, error) {
			time.Sleep(2 * time.Second)
			return func() { order = append(order, 2) }, sentinel
		})
		s.Go(func(_ context.Context) (stream.Callback, error) {
			time.Sleep(time.Second)
			return func() { order = append(order, 3) }, nil
		})

		require.ErrorIs(t, s.Wait(), sentinel)
		assert.Equal(t, []int{1, 3}, order, "an errored slot is skipped, not a halt")
	})
}

func TestStream_WithContext_CancelReachesInflightProducers(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		parent, cancel := context.WithCancel(context.Background())
		s := stream.New().WithContext(parent)

		start := time.Now()
		var cancelledAfter time.Duration
		s.Go(func(ctx context.Context) (stream.Callback, error) {
			select {
			case <-time.After(time.Hour):
				return nil, nil
			case <-ctx.Done():
				cancelledAfter = time.Since(start)
				return nil, ctx.Err()
			}
		})

		time.Sleep(time.Second)
		cancel()

		require.ErrorIs(t, s.Wait(), context.Canceled)
		assert.Equal(t, time.Second, cancelledAfter, "cancelling the parent must reach in-flight producers")
	})
}

func TestStream_WithContext_PanicsOnNil(t *testing.T) {
	//nolint:staticcheck // deliberately passing a nil context
	assert.Panics(t, func() { stream.New().WithContext(nil) })
}

func TestStream_WithContext_PanicsAfterGo(t *testing.T) {
	s := stream.New()
	s.Go(func(_ context.Context) (stream.Callback, error) { return nil, nil })

	assert.Panics(t, func() { s.WithContext(context.Background()) })
	require.NoError(t, s.Wait())
}

func TestStream_WithCancelOnError_CancelsInflightProducers(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := stream.New().WithMaxGoroutines(4).WithCancelOnError()
		sentinel := errors.New("producer failed")

		start := time.Now()
		var cancelledAfter time.Duration
		var cause error
		s.Go(func(ctx context.Context) (stream.Callback, error) {
			select {
			case <-time.After(time.Hour):
				return nil, nil
			case <-ctx.Done():
				cancelledAfter = time.Since(start)
				cause = context.Cause(ctx)
				return nil, ctx.Err()
			}
		})
		s.Go(func(_ context.Context) (stream.Callback, error) {
			time.Sleep(time.Second)
			return nil, sentinel
		})

		require.ErrorIs(t, s.Wait(), sentinel)
		assert.Equal(t, time.Second, cancelledAfter, "in-flight producers must be cancelled as soon as a sibling errors")
		assert.Same(t, sentinel, cause, "the triggering error must be the cancellation cause")
	})
}

func TestStream_WithoutCancelOnError_ErrorDoesNotCancelSiblings(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := stream.New().WithMaxGoroutines(2)

		var siblingErr error
		s.Go(func(ctx context.Context) (stream.Callback, error) {
			time.Sleep(2 * time.Second)
			siblingErr = ctx.Err()
			return nil, nil
		})
		s.Go(func(_ context.Context) (stream.Callback, error) {
			return nil, errors.New("producer failed")
		})

		require.Error(t, s.Wait())
		assert.NoError(t, siblingErr)
	})
}

func TestStream_WithCancelOnError_PanicsAfterGo(t *testing.T) {
	s := stream.New()
	s.Go(func(_ context.Context) (stream.Callback, error) { return nil, nil })

	assert.Panics(t, func() { s.WithCancelOnError() })
	require.NoError(t, s.Wait())
}

func TestStream_Reset_AllowsSecondBatch(t *testing.T) {
	s := stream.New().WithMaxGoroutines(2)

	var order []int
	s.Go(func(_ context.Context) (stream.Callback, error) {
		return nil, errors.New("first batch failed")
	})
	require.Error(t, s.Wait())

	s.Reset()
	for i := range 3 {
		s.Go(func(_ context.Context) (stream.Callback, error) {
			return func() { order = append(order, i) }, nil
		})
	}

	require.NoError(t, s.Wait(), "Reset must clear errors from the previous batch")
	assert.Equal(t, []int{0, 1, 2}, order)
}

func TestStream_Reset_RenewsContext(t *testing.T) {
	s := stream.New()
	require.NoError(t, s.Wait())

	s.Reset()
	var producerErr error
	s.Go(func(ctx context.Context) (stream.Callback, error) {
		producerErr = ctx.Err()
		return nil, nil
	})

	require.NoError(t, s.Wait())
	assert.NoError(t, producerErr, "producers after Reset must not see the previous batch's cancelled context")
}

func TestStream_Reset_AfterPanicAllowsReuse(t *testing.T) {
	s := stream.New()
	s.Go(func(_ context.Context) (stream.Callback, error) { panic("boom") })
	require.Panics(t, func() { _ = s.Wait() })

	s.Reset()
	var called atomic.Bool
	s.Go(func(_ context.Context) (stream.Callback, error) {
		return func() { called.Store(true) }, nil
	})

	require.NotPanics(t, func() { require.NoError(t, s.Wait()) })
	assert.True(t, called.Load())
}

func TestStream_Reset_PreservesCancelOnError(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := stream.New().WithMaxGoroutines(2).WithCancelOnError()
		require.NoError(t, s.Wait())
		s.Reset()

		var cancelled bool
		s.Go(func(ctx context.Context) (stream.Callback, error) {
			select {
			case <-time.After(time.Hour):
			case <-ctx.Done():
				cancelled = true
			}
			return nil, nil
		})
		s.Go(func(_ context.Context) (stream.Callback, error) {
			return nil, errors.New("producer failed")
		})

		require.Error(t, s.Wait())
		assert.True(t, cancelled)
	})
}
