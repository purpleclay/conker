package daemon

import (
	"context"
	"math"

	"github.com/purpleclay/conker/pool"
)

// Pool runs long-lived workers ("daemons") until told to stop. Create one
// with [New]; the zero value is not usable.
//
// Concurrency is effectively unbounded — [Pool.Spawn] never blocks waiting
// for a slot.
//
// Example:
//
//	d := daemon.New(ctx)
//	d.Spawn(func(ctx context.Context) {
//	    for {
//	        select {
//	        case <-ctx.Done():
//	            return
//	        case job := <-jobs:
//	            process(job)
//	        }
//	    }
//	})
//	// ... later, on shutdown:
//	if err := d.Stop(shutdownCtx); err != nil {
//	    log.Println("daemon shutdown:", err)
//	}
type Pool struct {
	pool   *pool.Pool
	cancel context.CancelFunc
}

// New returns a Pool whose daemons run with a context derived from ctx.
// Cancelling ctx — or calling [Pool.Stop] — cancels the context passed to
// every daemon.
func New(ctx context.Context) *Pool {
	ctx, cancel := context.WithCancel(ctx)
	return &Pool{
		pool:   pool.New().WithMaxGoroutines(math.MaxInt).WithContext(ctx),
		cancel: cancel,
	}
}

// Spawn starts a long-lived worker. fn must observe ctx.Done() and return
// promptly once it fires.
//
// Spawn returns immediately; the worker runs in its own goroutine. It is safe
// to call Spawn from within a running daemon — [Pool.Stop] waits for the
// transitive closure of all spawned daemons.
func (d *Pool) Spawn(fn func(context.Context)) {
	d.pool.Go(func(ctx context.Context) error {
		fn(ctx)
		return nil
	})
}

// Stop cancels the context passed to every daemon and waits for them all to
// return, bounded by ctx. Any panics recovered from daemons are returned as
// [errors.Join] of *[panics.Recovered] values.
//
// If ctx is cancelled before all daemons return, Stop returns ctx.Err();
// daemons continue running in the background until they observe cancellation.
func (d *Pool) Stop(ctx context.Context) error {
	d.cancel()

	done := make(chan error, 1)
	go func() { done <- d.pool.Wait() }()

	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}
