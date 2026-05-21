package pool

import (
	"context"
	"errors"
	"runtime"
	"sync"

	"github.com/purpleclay/conker/panics"
)

// Pool is a bounded, panic-safe task runner. Create one with [New]; the zero
// value is not usable.
//
// Tasks are spawned via [sync.WaitGroup.Go] with no internal task channel.
// A buffered channel semaphore enforces the concurrency limit. Because there
// is no channel to close, running tasks may safely call [Pool.Go] to submit
// child tasks — [Pool.Wait] waits for the transitive closure of all work.
//
// Example — basic usage:
//
//	p := pool.New().WithMaxGoroutines(4)
//	for _, item := range items {
//	    p.Go(func(ctx context.Context) error {
//	        return process(ctx, item)
//	    })
//	}
//	if err := p.Wait(); err != nil {
//	    log.Fatal(err)
//	}
//
// Example — recursive submission (tasks spawning child tasks):
//
//	p := pool.New()
//	p.Go(func(ctx context.Context) error {
//	    for _, child := range batch {
//	        p.Go(func(ctx context.Context) error {
//	            return handleItem(ctx, child)
//	        })
//	    }
//	    return nil
//	})
//	p.Wait()
type Pool struct {
	sem    chan struct{}
	wg     sync.WaitGroup
	ctx    context.Context
	cancel context.CancelCauseFunc

	// cfgMu guards sem and started, preventing reconfiguration after the first
	// Go call and ensuring each goroutine captures a stable semaphore reference.
	cfgMu   sync.Mutex
	started bool

	mu   sync.Mutex
	errs []error
}

// New returns a Pool with a default concurrency limit of [runtime.GOMAXPROCS](0).
func New() *Pool {
	ctx, cancel := context.WithCancelCause(context.Background())
	return (&Pool{
		ctx:    ctx,
		cancel: cancel,
	}).WithMaxGoroutines(runtime.GOMAXPROCS(0))
}

// WithMaxGoroutines sets the maximum number of goroutines that may run
// concurrently. It panics if n ≤ 0 or if called after the first [Pool.Go].
//
// Warning: if tasks recursively submit child tasks and n is small relative to
// the recursion depth, every slot may be held by a parent waiting to submit a
// child while no child can start — a deadlock. The default of
// runtime.GOMAXPROCS(0) is safe for typical workloads.
func (p *Pool) WithMaxGoroutines(n int) *Pool {
	if n <= 0 {
		panic("pool: WithMaxGoroutines requires n > 0")
	}
	p.cfgMu.Lock()
	defer p.cfgMu.Unlock()
	if p.started {
		panic("pool: WithMaxGoroutines must be called before Go")
	}
	p.sem = make(chan struct{}, n)
	return p
}

// Go submits fn as a task. It blocks until a goroutine slot is available.
// The task receives the pool's context.
//
// Go is safe to call from within a running task. Wait will wait for all
// submitted tasks, including those submitted recursively.
//
// Go panics if called on a zero-value Pool; use [New] or call
// [Pool.WithMaxGoroutines] before submitting tasks.
func (p *Pool) Go(fn func(context.Context) error) {
	p.cfgMu.Lock()
	sem := p.sem
	if sem == nil {
		p.cfgMu.Unlock()
		panic("pool: use New() or call WithMaxGoroutines before Go")
	}
	p.started = true
	p.cfgMu.Unlock()

	sem <- struct{}{}
	p.wg.Go(func() {
		defer func() { <-sem }()
		p.runTask(fn)
	})
}

func (p *Pool) runTask(fn func(context.Context) error) {
	var pc panics.Catcher
	var err error

	pc.Try(func() {
		err = fn(p.ctx)
	})

	if r := pc.Recovered(); r != nil {
		err = r
	}

	if err != nil {
		p.mu.Lock()
		p.errs = append(p.errs, err)
		p.mu.Unlock()
	}
}

// Wait blocks until every submitted task — including tasks submitted
// recursively by running tasks — has completed. It returns [errors.Join] of
// all task errors. Panics from tasks are captured and returned as
// [*panics.Recovered] errors rather than crashing the process.
func (p *Pool) Wait() error {
	p.wg.Wait()
	p.cancel(nil)

	p.mu.Lock()
	defer p.mu.Unlock()
	return errors.Join(p.errs...)
}
