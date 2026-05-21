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

	"github.com/purpleclay/conker/panics"
	"github.com/purpleclay/conker/pool"
)

func TestPool_Go_ExecutesTask(t *testing.T) {
	p := pool.New()

	var ran atomic.Bool
	p.Go(func(_ context.Context) error {
		ran.Store(true)
		return nil
	})

	require.NoError(t, p.Wait())
	assert.True(t, ran.Load())
}

func TestPool_Wait_CollectsErrors(t *testing.T) {
	p := pool.New()

	sentinel := errors.New("task failed")
	p.Go(func(_ context.Context) error { return sentinel })

	err := p.Wait()
	require.Error(t, err)
	assert.ErrorIs(t, err, sentinel)
}

func TestPool_Wait_JoinsMultipleErrors(t *testing.T) {
	p := pool.New()

	errA := errors.New("error a")
	errB := errors.New("error b")
	p.Go(func(_ context.Context) error { return errA })
	p.Go(func(_ context.Context) error { return errB })

	err := p.Wait()
	require.Error(t, err)
	assert.ErrorIs(t, err, errA)
	assert.ErrorIs(t, err, errB)
}

func TestPool_PanicCapturedAsError(t *testing.T) {
	p := pool.New()
	p.Go(func(_ context.Context) error { panic("something went wrong") })

	err := p.Wait()
	require.Error(t, err)
	assert.ErrorIs(t, err, panics.ErrPanic)

	var r *panics.Recovered
	require.ErrorAs(t, err, &r)
	assert.Equal(t, "something went wrong", r.Value)
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

func TestPool_GoCtx_ReturnsNilOnSuccessfulSubmission(t *testing.T) {
	p := pool.New()

	var ran atomic.Bool
	err := p.GoCtx(context.Background(), func(_ context.Context) error {
		ran.Store(true)
		return nil
	})

	require.NoError(t, p.Wait())
	assert.NoError(t, err)
	assert.True(t, ran.Load())
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

func TestPool_RecursiveSubmission(t *testing.T) {
	p := pool.New()

	var count atomic.Int64
	p.Go(func(_ context.Context) error {
		for range 5 {
			p.Go(func(_ context.Context) error {
				count.Add(1)
				return nil
			})
		}
		return nil
	})

	require.NoError(t, p.Wait())
	assert.Equal(t, int64(5), count.Load())
}

func TestPool_Wait_WaitsForGrandchildren(t *testing.T) {
	p := pool.New()

	var count atomic.Int64
	// parent
	p.Go(func(_ context.Context) error {
		// child
		p.Go(func(_ context.Context) error {
			// grandchild
			p.Go(func(_ context.Context) error {
				count.Add(1)
				return nil
			})
			return nil
		})
		return nil
	})

	require.NoError(t, p.Wait())
	assert.Equal(t, int64(1), count.Load())
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

func TestResultPool_Go_CollectsResults(t *testing.T) {
	p := pool.NewWithResults[int]()

	for i := range 5 {
		p.Go(func(_ context.Context) (int, error) { return i, nil })
	}

	results, err := p.Wait()
	require.NoError(t, err)
	assert.Len(t, results, 5)
}

// TestResultPool_Wait_PreservesSubmissionOrder verifies that results are
// returned in submission order even when tasks complete in reverse order.
func TestResultPool_Wait_PreservesSubmissionOrder(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := pool.NewWithResults[int]()

		// Submit in order but complete in reverse: task 3 finishes first.
		p.Go(func(_ context.Context) (int, error) { time.Sleep(3 * time.Second); return 1, nil })
		p.Go(func(_ context.Context) (int, error) { time.Sleep(2 * time.Second); return 2, nil })
		p.Go(func(_ context.Context) (int, error) { time.Sleep(1 * time.Second); return 3, nil })

		results, err := p.Wait()
		require.NoError(t, err)
		assert.Equal(t, []int{1, 2, 3}, results)
	})
}

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

func TestResultPool_Wait_CollectsResultsEvenOnError(t *testing.T) {
	p := pool.NewWithResults[int]()

	sentinel := errors.New("task failed")
	for i := range 5 {
		p.Go(func(_ context.Context) (int, error) {
			if i%2 == 0 {
				return i, sentinel
			}
			return i, nil
		})
	}

	results, err := p.Wait()
	require.Error(t, err)
	assert.ErrorIs(t, err, sentinel)
	assert.Len(t, results, 5, "all results must be present regardless of task errors")
}

func TestResultPool_Wait_CollectsResultEvenOnPanic(t *testing.T) {
	p := pool.NewWithResults[int]()
	p.Go(func(_ context.Context) (int, error) { panic("task panicked") })

	results, err := p.Wait()
	require.Error(t, err)
	assert.ErrorIs(t, err, panics.ErrPanic)
	assert.Len(t, results, 1, "result entry must be recorded even when the task panics")
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
