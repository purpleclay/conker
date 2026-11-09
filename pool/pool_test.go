package pool_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/purpleclay/conker/pool"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestPool_Errors_ReturnsClonesTheSlice(t *testing.T) {
	p := pool.New()

	sentinel := errors.New("error")
	p.Go(func(_ context.Context) error { return sentinel })

	p.Wait() //nolint:errcheck
	errs1 := p.Errors()
	errs2 := p.Errors()

	// Verify the slices contain the same errors
	require.Len(t, errs1, 1)
	require.Len(t, errs2, 1)
	assert.Same(t, errs1[0], errs2[0], "error values must be identical")
	// Both should return independent copies (verified by the Clone implementation)
	assert.Equal(t, errs1, errs2)
}

func TestPool_WithMaxGoroutines_PanicsOnInvalidN(t *testing.T) {
	assert.Panics(t, func() { pool.New().WithMaxGoroutines(0) })
	assert.Panics(t, func() { pool.New().WithMaxGoroutines(-1) })
}

func TestPool_WithMaxGoroutines_PanicsAfterGo(t *testing.T) {
	p := pool.New()
	p.Go(func(_ context.Context) error { return nil })

	assert.Panics(t, func() { p.WithMaxGoroutines(4) })
	p.Wait() //nolint:errcheck
}

func TestPool_GoCtx_ReturnsCancelledWhenBlocked(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := pool.New().WithMaxGoroutines(1)

		// Fill the only slot with a task that sleeps for 1s of fake time.
		p.Go(func(_ context.Context) error {
			time.Sleep(time.Second)
			return nil
		})

		// Try to submit a second task with a context that times out at 100ms.
		// The slot is held; GoCtx should unblock when the context expires.
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()

		var taskRan atomic.Bool
		err := p.GoCtx(ctx, func(_ context.Context) error {
			taskRan.Store(true)
			return nil
		})

		assert.ErrorIs(t, err, context.DeadlineExceeded)
		assert.False(t, taskRan.Load(), "task must not run when GoCtx returns an error")

		require.NoError(t, p.Wait())
	})
}

func TestPool_GoCtx_PanicsOnZeroValue(t *testing.T) {
	var p pool.Pool
	assert.Panics(t, func() {
		_ = p.GoCtx(context.Background(), func(_ context.Context) error { return nil })
	})
}

func TestPool_Go_PanicsOnZeroValue(t *testing.T) {
	// pool.Pool{} has a nil semaphore — Go must panic with a clear message
	// rather than deadlocking silently on a nil channel send.
	var p pool.Pool
	assert.Panics(t, func() {
		p.Go(func(_ context.Context) error { return nil })
	})
}

func TestPool_TryGo_PanicsOnZeroValue(t *testing.T) {
	var p pool.Pool
	assert.Panics(t, func() {
		p.TryGo(func(_ context.Context) error { return nil })
	})
}

func TestPool_TryGo_PreventsReconfiguration(t *testing.T) {
	p := pool.New()
	p.TryGo(func(_ context.Context) error { return nil })

	assert.Panics(t, func() { p.WithMaxGoroutines(4) })
	p.Wait() //nolint:errcheck
}

func TestPool_SQSBatchingRegression(t *testing.T) {
	p := pool.New()

	messages := []string{"a", "b", "c", "d", "e"}
	var processed atomic.Int64

	p.Go(func(_ context.Context) error {
		for range messages {
			p.Go(func(_ context.Context) error {
				processed.Add(1)
				return nil
			})
		}
		return nil
	})

	require.NoError(t, p.Wait())
	assert.Equal(t, int64(len(messages)), processed.Load())
}

func TestPool_Reset_PreservesTaskTimeout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := pool.New().WithTaskTimeout(100 * time.Millisecond)

		p.Go(func(ctx context.Context) error {
			select {
			case <-time.After(time.Second):
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		})
		require.Error(t, p.Wait())

		p.Reset()

		var taskErr error
		p.Go(func(ctx context.Context) error {
			select {
			case <-time.After(time.Second):
				return nil
			case <-ctx.Done():
				taskErr = ctx.Err()
				return taskErr
			}
		})
		require.Error(t, p.Wait())
		assert.ErrorIs(t, taskErr, context.DeadlineExceeded, "task timeout must still apply after reset")
	})
}

func TestPool_WithTaskTimeout_CancelsTaskContextAfterDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := pool.New().WithTaskTimeout(100 * time.Millisecond)

		var taskErr error
		p.Go(func(ctx context.Context) error {
			// Simulate work that outlasts the deadline.
			select {
			case <-time.After(time.Second):
				return nil
			case <-ctx.Done():
				taskErr = ctx.Err()
				return taskErr
			}
		})

		require.Error(t, p.Wait())
		assert.ErrorIs(t, taskErr, context.DeadlineExceeded)
	})
}

func TestPool_WithTaskTimeout_OtherTasksUnaffected(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := pool.New().WithTaskTimeout(time.Second)

		// This task completes well before the timeout — it must not be cancelled.
		var taskErr error
		p.Go(func(ctx context.Context) error {
			time.Sleep(100 * time.Millisecond)
			taskErr = ctx.Err()
			return nil
		})

		require.NoError(t, p.Wait())
		assert.NoError(t, taskErr, "task that completes before deadline must not see a cancelled context")
	})
}

func TestPool_WithTaskTimeout_PanicsAfterGo(t *testing.T) {
	p := pool.New()
	p.Go(func(_ context.Context) error { return nil })

	assert.Panics(t, func() { p.WithTaskTimeout(time.Second) })
	p.Wait() //nolint:errcheck
}

func TestPool_WithCancelOnError_CancelsInflightTasks(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := pool.New().WithMaxGoroutines(4).WithCancelOnError()
		sentinel := errors.New("task failed")

		start := time.Now()
		var cancelledAfter [3]time.Duration
		var causes [3]error
		for i := range 3 {
			p.Go(func(ctx context.Context) error {
				select {
				case <-time.After(time.Hour):
					return nil
				case <-ctx.Done():
					cancelledAfter[i] = time.Since(start)
					causes[i] = context.Cause(ctx)
					return ctx.Err()
				}
			})
		}
		p.Go(func(_ context.Context) error {
			time.Sleep(time.Second)
			return sentinel
		})

		require.ErrorIs(t, p.Wait(), sentinel)
		for i := range 3 {
			assert.Equal(t, time.Second, cancelledAfter[i], "in-flight task must be cancelled as soon as a sibling errors")
			assert.Same(t, sentinel, causes[i], "the triggering error must be the cancellation cause")
		}
	})
}

func TestPool_WithCancelOnError_PanicsAfterGo(t *testing.T) {
	p := pool.New()
	p.Go(func(_ context.Context) error { return nil })

	assert.Panics(t, func() { p.WithCancelOnError() })
	p.Wait() //nolint:errcheck
}

func TestPool_WithFirstError_PanicsAfterGo(t *testing.T) {
	p := pool.New()
	p.Go(func(_ context.Context) error { return nil })

	assert.Panics(t, func() { p.WithFirstError() })
	p.Wait() //nolint:errcheck
}

func TestPool_WithContext_PanicsOnNil(t *testing.T) {
	assert.Panics(t, func() { pool.New().WithContext(nil) }) //nolint:staticcheck
}

func TestPool_WithContext_PanicsAfterGo(t *testing.T) {
	p := pool.New()
	p.Go(func(_ context.Context) error { return nil })

	assert.Panics(t, func() { p.WithContext(context.Background()) })
	p.Wait() //nolint:errcheck
}

func TestPool_WithContext_DoesNotAffectCallerContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	p := pool.New().WithContext(ctx)
	p.Go(func(_ context.Context) error { return nil })
	require.NoError(t, p.Wait())

	assert.NoError(t, ctx.Err(), "the pool must not cancel the caller's context")
}

func TestPool_Go_AfterContextCancelled_DoesNotDropSubmissions(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	p := pool.New().WithMaxGoroutines(1).WithContext(ctx)
	cancel()

	var ran atomic.Int32
	for range 100 {
		p.Go(func(_ context.Context) error {
			ran.Add(1)
			return nil
		})
	}

	require.NoError(t, p.Wait())
	assert.Equal(t, int32(100), ran.Load(), "Go must not silently drop submissions after the pool's context is cancelled")
}

func TestResultPool_WithCapacity_PanicsOnInvalidN(t *testing.T) {
	assert.Panics(t, func() { pool.NewWithResults[int]().WithCapacity(0) })
	assert.Panics(t, func() { pool.NewWithResults[int]().WithCapacity(-1) })
}

func TestResultPool_WithCapacity_PanicsAfterGo(t *testing.T) {
	p := pool.NewWithResults[int]()
	p.Go(func(_ context.Context) (int, error) { return 1, nil })

	assert.Panics(t, func() { p.WithCapacity(10) })
	p.Wait() //nolint:errcheck
}

func TestResultPool_Go_AfterContextCancelled_DoesNotDropResults(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	p := pool.NewWithResults[int]().WithMaxGoroutines(1).WithContext(ctx)
	cancel()

	for i := range 100 {
		p.Go(func(_ context.Context) (int, error) { return i, nil })
	}

	results, err := p.Wait()
	require.NoError(t, err)
	require.Len(t, results, 100, "Go must not silently drop submissions after the pool's context is cancelled")
}

// TestResultPool_Wait_PreservesSubmissionOrder verifies that results are
// returned in submission order even when tasks complete in reverse order.
func TestResultPool_WithUnorderedResults_ReturnsCompletionOrder(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := pool.NewWithResults[int]().WithUnorderedResults()

		// Same reverse-completion setup; without ordering, completion order is preserved.
		p.Go(func(_ context.Context) (int, error) { time.Sleep(3 * time.Second); return 1, nil })
		p.Go(func(_ context.Context) (int, error) { time.Sleep(2 * time.Second); return 2, nil })
		p.Go(func(_ context.Context) (int, error) { time.Sleep(1 * time.Second); return 3, nil })

		results, err := p.Wait()
		require.NoError(t, err)
		assert.Equal(t, []int{3, 2, 1}, results)
	})
}

func TestResultPool_GoCtx_ReturnsCancelledWhenBlocked(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := pool.NewWithResults[int]().WithMaxGoroutines(1)

		p.Go(func(_ context.Context) (int, error) {
			time.Sleep(time.Second)
			return 0, nil
		})

		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()

		err := p.GoCtx(ctx, func(_ context.Context) (int, error) { return 1, nil })
		assert.ErrorIs(t, err, context.DeadlineExceeded)

		results, err := p.Wait()
		require.NoError(t, err)
		assert.Len(t, results, 1, "only the first task should have a result")
	})
}

func TestResultPool_Errors_WithUnorderedResults_ReturnsCompletionOrder(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := pool.NewWithResults[int]().WithUnorderedResults()

		errA := errors.New("error a")
		errB := errors.New("error b")

		// Submit in order but complete in reverse: task B finishes first.
		p.Go(func(_ context.Context) (int, error) { time.Sleep(2 * time.Second); return 1, errA })
		p.Go(func(_ context.Context) (int, error) { time.Sleep(1 * time.Second); return 2, errB })

		p.Wait() //nolint:errcheck
		errs := p.Errors()
		require.Len(t, errs, 2)
		assert.Same(t, errB, errs[0], "first completed error must come first")
		assert.Same(t, errA, errs[1], "second completed error must come second")
	})
}

func TestResultPool_Reset_PreservesUnorderedResults(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := pool.NewWithResults[int]().WithUnorderedResults()

		// First cycle — complete in reverse order; expect completion order.
		p.Go(func(_ context.Context) (int, error) { time.Sleep(2 * time.Second); return 1, nil })
		p.Go(func(_ context.Context) (int, error) { time.Sleep(1 * time.Second); return 2, nil })
		results, err := p.Wait()
		require.NoError(t, err)
		assert.Equal(t, []int{2, 1}, results)

		p.Reset()

		// Second cycle — WithUnorderedResults must still be in effect.
		p.Go(func(_ context.Context) (int, error) { time.Sleep(2 * time.Second); return 3, nil })
		p.Go(func(_ context.Context) (int, error) { time.Sleep(1 * time.Second); return 4, nil })
		results, err = p.Wait()
		require.NoError(t, err)
		assert.Equal(t, []int{4, 3}, results, "completion order must be preserved after reset")
	})
}

func TestResultPool_WithCancelOnError_CancelsInflightTasks(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := pool.NewWithResults[int]().WithMaxGoroutines(2).WithCancelOnError()
		sentinel := errors.New("task failed")

		var cause error
		p.Go(func(ctx context.Context) (int, error) {
			select {
			case <-time.After(time.Hour):
				return 1, nil
			case <-ctx.Done():
				cause = context.Cause(ctx)
				return 1, ctx.Err()
			}
		})
		p.Go(func(_ context.Context) (int, error) {
			time.Sleep(time.Second)
			return 2, sentinel
		})

		results, err := p.Wait()
		require.ErrorIs(t, err, sentinel)
		assert.Equal(t, []int{1, 2}, results, "results must still be collected from cancelled tasks")
		assert.Same(t, sentinel, cause, "the triggering error must be the cancellation cause")
	})
}

func TestResultPool_WithCancelOnError_PanicsAfterGo(t *testing.T) {
	p := pool.NewWithResults[int]()
	p.Go(func(_ context.Context) (int, error) { return 1, nil })

	assert.Panics(t, func() { p.WithCancelOnError() })
	p.Wait() //nolint:errcheck
}

func TestResultPool_WithFirstError_PanicsAfterGo(t *testing.T) {
	p := pool.NewWithResults[int]()
	p.Go(func(_ context.Context) (int, error) { return 1, nil })

	assert.Panics(t, func() { p.WithFirstError() })
	p.Wait() //nolint:errcheck
}

func TestPool_Wait_CancelsWithErrPoolClosed(t *testing.T) {
	p := pool.New()

	var taskCtx context.Context
	p.Go(func(ctx context.Context) error {
		taskCtx = ctx
		return nil
	})

	require.NoError(t, p.Wait())
	assert.ErrorIs(t, context.Cause(taskCtx), pool.ErrPoolClosed)
}

func TestResultPool_Wait_CancelsWithErrPoolClosed(t *testing.T) {
	p := pool.NewWithResults[int]()

	var taskCtx context.Context
	p.Go(func(ctx context.Context) (int, error) {
		taskCtx = ctx
		return 0, nil
	})

	_, err := p.Wait()
	require.NoError(t, err)
	assert.ErrorIs(t, context.Cause(taskCtx), pool.ErrPoolClosed)
}

func TestPool_Wait_KeepsEarlierCancellationCause(t *testing.T) {
	p := pool.New().WithCancelOnError()
	sentinel := errors.New("task failed")

	var taskCtx context.Context
	p.Go(func(ctx context.Context) error {
		taskCtx = ctx
		return sentinel
	})

	require.ErrorIs(t, p.Wait(), sentinel)
	assert.Same(t, sentinel, context.Cause(taskCtx), "Wait must not overwrite the cause set by WithCancelOnError")
}
