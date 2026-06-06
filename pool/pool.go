package pool

import (
	"cmp"
	"context"
	"errors"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"time"

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

	// cfgMu guards sem, started, and taskTimeout, preventing reconfiguration
	// after the first Go call and ensuring each goroutine captures a stable
	// semaphore reference.
	cfgMu       sync.Mutex
	started     bool
	taskTimeout time.Duration

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

// WithTaskTimeout sets a maximum duration for each individual task. The task's
// context is cancelled after d, causing well-behaved tasks that observe
// ctx.Done() to return [context.DeadlineExceeded]. Other concurrent tasks are
// unaffected. It panics if d ≤ 0 or if called after the first [Pool.Go].
//
// Tasks must observe ctx.Done() to benefit from the timeout; a task that
// ignores its context will run to completion regardless of the deadline.
//
// Example:
//
//	p := pool.New().WithTaskTimeout(5 * time.Second)
//	p.Go(func(ctx context.Context) error {
//	    select {
//	    case result := <-slowExternalCall():
//	        return process(result)
//	    case <-ctx.Done():
//	        return ctx.Err() // context.DeadlineExceeded after 5s
//	    }
//	})
func (p *Pool) WithTaskTimeout(d time.Duration) *Pool {
	if d <= 0 {
		panic("pool: WithTaskTimeout requires d > 0")
	}
	p.cfgMu.Lock()
	defer p.cfgMu.Unlock()
	if p.started {
		panic("pool: WithTaskTimeout must be called before Go")
	}
	p.taskTimeout = d
	return p
}

// GoCtx submits fn as a task, returning [context.Err] if ctx is cancelled
// while waiting for a goroutine slot. It returns nil on successful submission.
//
// This is the primary submission primitive for producers that need cooperative
// shutdown: when the producer's context is cancelled (e.g. due to a signal or
// parent timeout), GoCtx unblocks immediately rather than waiting indefinitely
// for a free slot.
//
// GoCtx panics if called on a zero-value Pool; use [New] or call
// [Pool.WithMaxGoroutines] before submitting tasks.
func (p *Pool) GoCtx(ctx context.Context, fn func(context.Context) error) error {
	p.cfgMu.Lock()
	sem := p.sem
	if sem == nil {
		p.cfgMu.Unlock()
		panic("pool: use New() or call WithMaxGoroutines before GoCtx")
	}
	p.started = true
	p.cfgMu.Unlock()

	select {
	case sem <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}

	p.wg.Go(func() {
		defer func() { <-sem }()
		p.runTask(fn)
	})
	return nil
}

// Go submits fn as a task. It blocks until a goroutine slot is available.
// The task receives the pool's context.
//
// Go is safe to call from within a running task. [Wait] will wait for all
// submitted tasks, including those submitted recursively.
//
// Go panics if called on a zero-value Pool; use [New] or call
// [Pool.WithMaxGoroutines] before submitting tasks.
func (p *Pool) Go(fn func(context.Context) error) {
	_ = p.GoCtx(p.ctx, fn)
}

func (p *Pool) runTask(fn func(context.Context) error) {
	var pc panics.Catcher
	var err error

	pc.Try(func() {
		taskCtx := p.ctx
		if p.taskTimeout > 0 {
			var cancel context.CancelFunc
			taskCtx, cancel = context.WithTimeout(taskCtx, p.taskTimeout)
			defer cancel()
		}
		err = fn(taskCtx)
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
// *[panics.Recovered] errors rather than crashing the process.
func (p *Pool) Wait() error {
	p.wg.Wait()
	p.cancel(nil)

	p.mu.Lock()
	defer p.mu.Unlock()
	return errors.Join(p.errs...)
}

// Reset clears all collected errors and reinitialises the internal context,
// allowing the pool to be reused for another batch of work. Configuration set
// via [Pool.WithMaxGoroutines] and [Pool.WithTaskTimeout] is preserved.
//
// Reset must not be called concurrently with [Pool.Go] or while tasks are
// running; call it only after [Pool.Wait] has returned.
func (p *Pool) Reset() {
	p.mu.Lock()
	p.errs = nil
	p.mu.Unlock()
	p.ctx, p.cancel = context.WithCancelCause(context.Background())
}

// Errors returns a slice of all errors collected from tasks. This provides
// access to the raw error slice for callers needing per-error filtering with
// [errors.As]. Returns nil if no errors occurred.
//
// Example — filtering errors by type:
//
//	p := pool.New()
//	// ... submit tasks
//	p.Wait()
//	for _, err := range p.Errors() {
//	    var taskErr *MyTaskError
//	    if errors.As(err, &taskErr) {
//	        log.Printf("task %s failed: %v", taskErr.ID, taskErr)
//	    }
//	}
func (p *Pool) Errors() []error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return slices.Clone(p.errs)
}

// indexedResult pairs a task result with its submission index, enabling
// submission-order sorting in ResultPool.Wait.
type indexedResult[T any] struct {
	idx int64
	val T
	err error
}

// ResultPool is a bounded, panic-safe task runner that collects typed results.
// Create one with [NewWithResults]; the zero value is not usable.
//
// Results are returned in submission order by default. Use
// [ResultPool.WithUnorderedResults] to skip the sort when order does not
// matter and result counts are large enough for the overhead to be noticeable.
//
// Results from errored tasks are always included in the output slice. The
// caller receives both the full result set and the joined errors from
// [ResultPool.Wait], and can filter as needed.
//
// Example — concurrent map with ordered results:
//
//	p := pool.NewWithResults[string]()
//	for _, id := range ids {
//	    p.Go(func(ctx context.Context) (string, error) {
//	        return fetch(ctx, id)
//	    })
//	}
//	results, err := p.Wait()
type ResultPool[T any] struct {
	pool    *Pool
	idx     atomic.Int64
	mu      sync.Mutex
	results []indexedResult[T]
	ordered bool
	capHint int
}

// NewWithResults returns a ResultPool with submission-order results and a
// default concurrency limit of [runtime.GOMAXPROCS](0).
func NewWithResults[T any]() *ResultPool[T] {
	return &ResultPool[T]{
		pool:    New(),
		ordered: true,
	}
}

// WithMaxGoroutines sets the maximum number of goroutines that may run
// concurrently. It panics if n ≤ 0 or if called after the first Go.
func (p *ResultPool[T]) WithMaxGoroutines(n int) *ResultPool[T] {
	p.pool.WithMaxGoroutines(n)
	return p
}

// WithTaskTimeout sets a maximum duration for each individual task.
// See [Pool.WithTaskTimeout] for full documentation.
func (p *ResultPool[T]) WithTaskTimeout(d time.Duration) *ResultPool[T] {
	p.pool.WithTaskTimeout(d)
	return p
}

// WithCapacity pre-allocates the internal results slice to n, eliminating
// repeated slice growth allocations when the number of tasks is known
// upfront. The capacity is retained across [ResultPool.Reset] calls.
// It panics if n ≤ 0 or if called after the first [ResultPool.Go].
func (p *ResultPool[T]) WithCapacity(n int) *ResultPool[T] {
	if n <= 0 {
		panic("pool: WithCapacity requires n > 0")
	}
	p.pool.cfgMu.Lock()
	defer p.pool.cfgMu.Unlock()
	if p.pool.started {
		panic("pool: WithCapacity must be called before Go")
	}
	p.results = make([]indexedResult[T], 0, n)
	p.capHint = n
	return p
}

// WithUnorderedResults switches the pool to completion-order results,
// skipping the submission-order sort in [ResultPool.Wait]. Use this when
// result order does not matter and the result count is large enough for the
// sort overhead to be noticeable.
func (p *ResultPool[T]) WithUnorderedResults() *ResultPool[T] {
	p.ordered = false
	return p
}

// GoCtx submits fn as a task, returning [context.Err] if ctx is cancelled
// while waiting for a goroutine slot. The result is always recorded, even
// when fn returns an error.
func (p *ResultPool[T]) GoCtx(ctx context.Context, fn func(context.Context) (T, error)) error {
	idx := p.idx.Add(1) - 1
	return p.pool.GoCtx(ctx, func(taskCtx context.Context) error {
		var (
			val T
			err error
			pc  panics.Catcher
		)
		pc.Try(func() {
			val, err = fn(taskCtx)
		})
		if r := pc.Recovered(); r != nil {
			err = r
		}
		p.mu.Lock()
		p.results = append(p.results, indexedResult[T]{idx: idx, val: val, err: err})
		p.mu.Unlock()
		return err
	})
}

// Go submits fn as a task. It blocks until a goroutine slot is available.
func (p *ResultPool[T]) Go(fn func(context.Context) (T, error)) {
	_ = p.GoCtx(p.pool.ctx, fn)
}

// Wait blocks until all tasks have completed and returns the collected results
// alongside [errors.Join] of all task errors. Results from errored tasks are
// included — no result is silently dropped.
//
// By default results are sorted into submission order before returning.
// With [ResultPool.WithUnorderedResults] they are returned in completion order.
func (p *ResultPool[T]) Wait() ([]T, error) {
	err := p.pool.Wait()

	p.mu.Lock()
	defer p.mu.Unlock()

	if p.ordered {
		slices.SortFunc(p.results, func(a, b indexedResult[T]) int {
			return cmp.Compare(a.idx, b.idx)
		})
	}

	out := make([]T, len(p.results))
	for i, r := range p.results {
		out[i] = r.val
	}
	return out, err
}

// Reset clears all collected results and errors, reinitialises the submission
// index, and delegates to [Pool.Reset] to reinitialise the internal context.
// Configuration set via [ResultPool.WithMaxGoroutines],
// [ResultPool.WithTaskTimeout], [ResultPool.WithUnorderedResults], and
// [ResultPool.WithCapacity] is preserved.
//
// Reset must not be called concurrently with [ResultPool.Go] or while tasks
// are running; call it only after [ResultPool.Wait] has returned.
func (p *ResultPool[T]) Reset() {
	p.mu.Lock()
	if p.capHint > 0 {
		if cap(p.results) < p.capHint {
			p.results = make([]indexedResult[T], 0, p.capHint)
		} else {
			p.results = p.results[:0]
		}
	} else {
		p.results = nil
	}
	p.mu.Unlock()
	p.idx.Store(0)
	p.pool.Reset()
}

// Errors returns a slice of non-nil errors collected from tasks in submission
// order by default, consistent with the result ordering from [ResultPool.Wait].
// With [ResultPool.WithUnorderedResults] errors are returned in completion order.
// Returns nil if no errors occurred.
//
// See [Pool.Errors] for filtering errors by type with [errors.As].
func (p *ResultPool[T]) Errors() []error {
	p.mu.Lock()
	results := slices.Clone(p.results)
	ordered := p.ordered
	p.mu.Unlock()

	if ordered {
		slices.SortFunc(results, func(a, b indexedResult[T]) int {
			return cmp.Compare(a.idx, b.idx)
		})
	}

	var errs []error
	for _, r := range results {
		if r.err != nil {
			errs = append(errs, r.err)
		}
	}
	return errs
}
