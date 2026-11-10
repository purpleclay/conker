package daemon_test

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

	"github.com/purpleclay/conker/daemon"
	"github.com/purpleclay/conker/panics"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestPool_Stop_NoDaemons(t *testing.T) {
	d := daemon.New(context.Background())
	require.NoError(t, d.Stop(context.Background()))
}

func TestPool_Spawn_ReturnsImmediately(t *testing.T) {
	d := daemon.New(context.Background())

	block := make(chan struct{})
	d.Spawn(func(_ context.Context) error {
		<-block
		return nil
	})

	// Spawn must not block waiting for the daemon to finish.
	close(block)
	require.NoError(t, d.Stop(context.Background()))
}

func TestPool_Stop_WaitsForAllDaemonsToReturn(t *testing.T) {
	d := daemon.New(context.Background())

	const n = 3
	started := make(chan struct{}, n)
	var stopped atomic.Int32

	for range n {
		d.Spawn(func(ctx context.Context) error {
			started <- struct{}{}
			<-ctx.Done()
			stopped.Add(1)
			return nil
		})
	}

	for range n {
		<-started
	}

	require.NoError(t, d.Stop(context.Background()))
	assert.Equal(t, int32(n), stopped.Load())
}

func TestPool_Stop_ReturnsCtxErrIfDeadlineExceeded(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := daemon.New(context.Background())

		started := make(chan struct{})
		exited := make(chan struct{})
		d.Spawn(func(ctx context.Context) error {
			close(started)
			<-ctx.Done()
			// Slow to actually exit, so Stop's deadline fires first.
			time.Sleep(time.Second)
			close(exited)
			return nil
		})
		<-started

		stopCtx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()

		err := d.Stop(stopCtx)
		assert.ErrorIs(t, err, context.DeadlineExceeded)

		// Let the daemon actually finish so it doesn't leak past the test.
		<-exited
	})
}

func TestPool_Stop_PropagatesPanic(t *testing.T) {
	d := daemon.New(context.Background())

	d.Spawn(func(_ context.Context) error {
		panic("boom")
	})

	err := d.Stop(context.Background())
	require.Error(t, err)
	assert.ErrorIs(t, err, panics.ErrPanic)
}

func TestPool_New_ParentContextCancelsDaemons(t *testing.T) {
	parent, cancel := context.WithCancel(context.Background())

	d := daemon.New(parent)

	done := make(chan struct{})
	d.Spawn(func(ctx context.Context) error {
		<-ctx.Done()
		close(done)
		return nil
	})

	cancel()
	<-done

	require.NoError(t, d.Stop(context.Background()))
}

func TestPool_Spawn_SupportsRecursiveSpawn(t *testing.T) {
	d := daemon.New(context.Background())

	childDone := make(chan struct{})
	d.Spawn(func(_ context.Context) error {
		d.Spawn(func(ctx context.Context) error {
			<-ctx.Done()
			close(childDone)
			return nil
		})
		return nil
	})

	require.NoError(t, d.Stop(context.Background()))

	select {
	case <-childDone:
	default:
		t.Fatal("recursively spawned daemon was not awaited by Stop")
	}
}

func TestPool_Stop_ReturnsWorkerErrors(t *testing.T) {
	errA := errors.New("a failed")
	errB := errors.New("b failed")

	d := daemon.New(context.Background())
	d.Spawn(func(_ context.Context) error { return errA })
	d.Spawn(func(_ context.Context) error { return errB })

	err := d.Stop(context.Background())
	require.ErrorIs(t, err, errA)
	require.ErrorIs(t, err, errB)
	assert.NotErrorIs(t, err, panics.ErrPanic)
}

func TestPool_Stop_NilWorkerReturnContributesNoError(t *testing.T) {
	d := daemon.New(context.Background())
	d.Spawn(func(_ context.Context) error { return nil })
	d.Spawn(func(ctx context.Context) error {
		<-ctx.Done()
		return nil
	})

	require.NoError(t, d.Stop(context.Background()))
}

func TestPool_Spawn_WorkerErrorDoesNotCancelSiblings(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		wantErr := errors.New("gave up")

		d := daemon.New(context.Background())
		failed := make(chan struct{})
		d.Spawn(func(_ context.Context) error {
			defer close(failed)
			return wantErr
		})

		var siblingCancelled atomic.Bool
		siblingDone := make(chan struct{})
		d.Spawn(func(ctx context.Context) error {
			defer close(siblingDone)
			<-failed
			time.Sleep(time.Second) // give a wrongful cancellation time to land
			siblingCancelled.Store(ctx.Err() != nil)
			return nil
		})

		<-siblingDone
		assert.False(t, siblingCancelled.Load(), "a returned error cancelled a sibling daemon")
		require.ErrorIs(t, d.Stop(context.Background()), wantErr)
	})
}
