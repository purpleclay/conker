package daemon_test

import (
	"context"
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
	d.Spawn(func(_ context.Context) {
		<-block
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
		d.Spawn(func(ctx context.Context) {
			started <- struct{}{}
			<-ctx.Done()
			stopped.Add(1)
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
		d.Spawn(func(ctx context.Context) {
			close(started)
			<-ctx.Done()
			// Slow to actually exit, so Stop's deadline fires first.
			time.Sleep(time.Second)
			close(exited)
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

	d.Spawn(func(_ context.Context) {
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
	d.Spawn(func(ctx context.Context) {
		<-ctx.Done()
		close(done)
	})

	cancel()
	<-done

	require.NoError(t, d.Stop(context.Background()))
}

func TestPool_Spawn_SupportsRecursiveSpawn(t *testing.T) {
	d := daemon.New(context.Background())

	childDone := make(chan struct{})
	d.Spawn(func(_ context.Context) {
		d.Spawn(func(ctx context.Context) {
			<-ctx.Done()
			close(childDone)
		})
	})

	require.NoError(t, d.Stop(context.Background()))

	select {
	case <-childDone:
	default:
		t.Fatal("recursively spawned daemon was not awaited by Stop")
	}
}
