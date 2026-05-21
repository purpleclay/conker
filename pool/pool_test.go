package pool_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

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
