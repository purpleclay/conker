package stream

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/purpleclay/conker/panics"
)

// Callback is a function executed serially by the dispatcher in submission
// order after its producer returns. A nil Callback is a no-op.
type Callback func()

type callbackOrPanic struct {
	cb    Callback
	err   error
	panic *panics.Recovered
}

// Stream provides ordered concurrent processing. Create one with [New]; the
// zero value is not usable.
//
// Each call to [Stream.Go] spawns a producer goroutine concurrently. The
// producer returns a [Callback] which is then executed serially in submission
// order by an internal dispatcher. This guarantees that callbacks always run
// in the order their producers were submitted, regardless of when the producers
// finish.
//
// A producer that returns a non-nil error has its callback skipped; the
// remaining callbacks still run in order and [Stream.Wait] returns the joined
// errors. A panic is different: it halts the dispatcher and is re-panicked
// from Wait.
//
// [Stream.Wait] ends a batch: [Stream.Go] / [Stream.GoCtx] panic if called
// after it until [Stream.Reset] is called. Unlike [pool.Pool], callbacks must
// not submit further work — Stream has no recursive submission support. Use
// [pool.Pool] for workloads where tasks need to submit child tasks.
//
// Example:
//
//	s := stream.New().WithMaxGoroutines(4)
//	for _, item := range items {
//	    s.Go(func(ctx context.Context) (stream.Callback, error) {
//	        result, err := process(ctx, item)  // runs concurrently
//	        if err != nil {
//	            return nil, err
//	        }
//	        return func() {
//	            emit(result)  // runs in submission order
//	        }, nil
//	    })
//	}
//	err := s.Wait()
type Stream struct {
	sem       chan struct{}
	parentCtx context.Context
	ctx       context.Context
	cancel    context.CancelCauseFunc

	// submitted carries per-producer result channels in submission order.
	// The dispatcher reads them in order and blocks on each until the producer
	// writes its result.
	submitted    chan chan callbackOrPanic
	dispatchDone chan struct{}

	// errs is written only by the dispatcher, in submission order, and read by
	// Wait after dispatchDone is closed.
	errs []error

	// dispatchPC catches panics originating in callbacks. Producer panics are
	// stored directly in producerPanic to preserve the original stack without
	// double-wrapping inside a second *panics.Recovered.
	dispatchPC    panics.Catcher
	producerPanic atomic.Pointer[panics.Recovered]

	// producerWg tracks in-flight producer goroutines so Wait can drain them
	// before returning, even when the dispatcher exits early due to a panic.
	producerWg sync.WaitGroup

	// submitWg tracks GoCtx calls that were admitted (waited was still false
	// when they checked) but have not yet finished sending into submitted.
	// Wait blocks on submitWg before closing submitted, so the channel is
	// never closed while a send may still be in flight — the check of waited
	// and the submitWg.Add happen under the same cfgMu critical section as
	// the waited flip in Wait, so no GoCtx call can be admitted afterwards.
	submitWg sync.WaitGroup

	// cfgMu guards sem, parentCtx, ctx, cancelOnError, started, and waited,
	// preventing reconfiguration after the first Go call and making repeated
	// Wait calls a no-op.
	cfgMu         sync.Mutex
	started       bool
	waited        bool
	cancelOnError bool

	once sync.Once
}

// New returns a Stream with a default concurrency of [runtime.GOMAXPROCS](0).
func New() *Stream {
	parentCtx := context.Background()
	ctx, cancel := context.WithCancelCause(parentCtx)
	return &Stream{
		sem:       make(chan struct{}, runtime.GOMAXPROCS(0)),
		parentCtx: parentCtx,
		ctx:       ctx,
		cancel:    cancel,
	}
}

// WithMaxGoroutines sets the maximum number of producer goroutines that may
// run concurrently. It panics if n ≤ 0 or if called after the first
// [Stream.Go].
func (s *Stream) WithMaxGoroutines(n int) *Stream {
	if n <= 0 {
		panic("stream: WithMaxGoroutines requires n > 0")
	}
	s.cfgMu.Lock()
	defer s.cfgMu.Unlock()
	if s.started {
		panic("stream: WithMaxGoroutines must be called before Go")
	}
	s.sem = make(chan struct{}, n)
	return s
}

// WithContext sets the parent context for the stream. The context passed to
// every producer is derived from ctx, so cancelling ctx cancels in-flight and
// future producers. It does not affect ctx itself.
//
// Cancellation does not stop [Stream.Go] from accepting producers; they start
// with an already-cancelled context and should return promptly.
//
// It panics if ctx is nil or if called after the first [Stream.Go].
//
// Example:
//
//	s := stream.New().WithContext(ctx) // ctx is the caller's request context
//	s.Go(func(ctx context.Context) (stream.Callback, error) {
//	    return fetch(ctx) // cancelled if the request is cancelled
//	})
func (s *Stream) WithContext(ctx context.Context) *Stream {
	if ctx == nil {
		panic("stream: WithContext requires non-nil context")
	}
	s.cfgMu.Lock()
	defer s.cfgMu.Unlock()
	if s.started {
		panic("stream: WithContext must be called before Go")
	}
	s.cancel(nil)
	s.parentCtx = ctx
	s.ctx, s.cancel = context.WithCancelCause(ctx)
	return s
}

// WithCancelOnError cancels the context passed to every producer as soon as
// any producer returns a non-nil error or panics. The triggering error (or
// *[panics.Recovered]) is set as the cancellation cause, which producers can
// read with [context.Cause]. If the context was already cancelled, for example
// by a parent supplied via [Stream.WithContext], the existing cause is kept.
//
// Callbacks are not cancelled: those whose producers succeeded still run in
// submission order, and every producer error is still returned from
// [Stream.Wait].
//
// It panics if called after the first [Stream.Go].
func (s *Stream) WithCancelOnError() *Stream {
	s.cfgMu.Lock()
	defer s.cfgMu.Unlock()
	if s.started {
		panic("stream: WithCancelOnError must be called before Go")
	}
	s.cancelOnError = true
	return s
}

func (s *Stream) start() {
	s.submitted = make(chan chan callbackOrPanic, cap(s.sem))
	s.dispatchDone = make(chan struct{})
	go func() {
		defer close(s.dispatchDone)
		s.dispatchPC.Try(func() {
			for slot := range s.submitted {
				cop := <-slot
				if cop.panic != nil {
					// Store the producer panic directly rather than re-panicking
					// here, which would wrap it in a second *panics.Recovered and
					// lose the original panic site in the stack trace.
					s.producerPanic.CompareAndSwap(nil, cop.panic)
					return
				}
				if cop.err != nil {
					// Unlike a panic, an error skips only its own slot.
					s.errs = append(s.errs, cop.err)
					continue
				}
				if cop.cb != nil {
					cop.cb()
				}
				// nil callback is a no-op.
			}
		})

		// A panic above (producer or callback) stopped the loop before
		// submitted was closed. Keep draining so any submitter still blocked
		// on `submitted <- slot` unblocks; the corresponding slot is always
		// writable since producers write to it exactly once, buffered.
		for slot := range s.submitted {
			cop := <-slot
			if cop.panic != nil {
				// A producer panic discovered during drain still outranks an
				// earlier callback panic — keep that priority intact.
				s.producerPanic.CompareAndSwap(nil, cop.panic)
			}
		}
	}()
}

// GoCtx submits fn as a producer, returning [context.Err] if ctx is cancelled
// while waiting for a goroutine slot. It returns nil on successful submission.
//
// The producer receives the stream's internal context, not the caller's ctx.
// The caller's ctx is used only to unblock the submission wait under
// backpressure.
//
// GoCtx panics if called on a zero-value Stream, or if called after
// [Stream.Wait] without an intervening [Stream.Reset]. Use [pool.Pool] for
// workloads where tasks submit further work.
func (s *Stream) GoCtx(ctx context.Context, fn func(context.Context) (Callback, error)) error {
	s.cfgMu.Lock()
	if s.sem == nil {
		s.cfgMu.Unlock()
		panic("stream: use New() before calling GoCtx")
	}
	if s.waited {
		s.cfgMu.Unlock()
		panic("stream: Go called after Wait — call Reset to reuse the Stream")
	}
	s.started = true
	s.submitWg.Add(1)
	producerCtx, cancelOnError := s.ctx, s.cancelOnError
	s.cfgMu.Unlock()
	defer s.submitWg.Done()

	s.once.Do(s.start)

	select {
	case s.sem <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}

	slot := make(chan callbackOrPanic, 1)
	s.submitted <- slot

	s.producerWg.Add(1)
	go func() {
		defer s.producerWg.Done()
		defer func() { <-s.sem }()

		var pc panics.Catcher
		var cop callbackOrPanic
		pc.Try(func() { cop.cb, cop.err = fn(producerCtx) })

		if r := pc.Recovered(); r != nil {
			cop = callbackOrPanic{panic: r}
		}
		if cancelOnError {
			if cop.panic != nil {
				s.cancel(cop.panic)
			} else if cop.err != nil {
				s.cancel(cop.err)
			}
		}
		slot <- cop
	}()
	return nil
}

// Go submits fn as a producer. It blocks until a goroutine slot is available.
// The producer receives the stream's internal context.
//
// Go panics if called on a zero-value Stream, or if called after
// [Stream.Wait] without an intervening [Stream.Reset]. Use [pool.Pool] for
// workloads where tasks submit further work.
func (s *Stream) Go(fn func(context.Context) (Callback, error)) {
	// context.Background(), not the stream's context: Go blocks until a slot
	// is free and never bails out, even once the stream has been cancelled.
	_ = s.GoCtx(context.Background(), fn)
}

// Wait blocks until all producers have finished and all callbacks have run in
// submission order. It returns [errors.Join] of every producer error, in
// submission order, or nil if none failed.
//
// Errors and panics are handled differently. A producer error skips only that
// producer's callback; the remaining callbacks still run in order. A panic in
// any producer or callback halts the dispatcher immediately, all pending
// callbacks are dropped, and Wait re-panics with a *[panics.Recovered]. A
// panic has no sensible slot to resume from, so halting is the only coherent
// response.
//
// Once Wait returns, [Stream.Go] / [Stream.GoCtx] panic on any further call,
// including one made from a callback, until [Stream.Reset] is called. Stream
// has no recursive submission support — use [pool.Pool] for workloads where
// tasks need to submit child tasks. A second call to Wait is a no-op that
// returns nil. Wait is also a no-op when no tasks have been submitted.
func (s *Stream) Wait() error {
	s.cfgMu.Lock()
	if s.waited {
		s.cfgMu.Unlock()
		return nil
	}
	s.waited = true
	s.cfgMu.Unlock()

	// No GoCtx call admitted before the flip above can still be sending into
	// submitted once this returns, so closing it below is race-free.
	s.submitWg.Wait()

	defer s.producerWg.Wait()

	if s.submitted == nil {
		s.cancel(nil)
		return nil
	}
	close(s.submitted)
	<-s.dispatchDone
	s.cancel(nil)

	// Producer panic takes priority: it has the original stack trace intact.
	if r := s.producerPanic.Load(); r != nil {
		panic(r)
	}
	s.dispatchPC.Repanic()
	return errors.Join(s.errs...)
}

// Reset prepares the stream for another batch of producers after
// [Stream.Wait] has returned or panicked. It clears collected errors and
// captured panics and derives a fresh context from the parent. Configuration
// set via [Stream.WithMaxGoroutines], [Stream.WithContext], and
// [Stream.WithCancelOnError] is preserved.
//
// Reset must not be called concurrently with [Stream.Go], [Stream.GoCtx], or
// [Stream.Wait].
func (s *Stream) Reset() {
	s.cfgMu.Lock()
	defer s.cfgMu.Unlock()
	s.submitted = nil
	s.dispatchDone = nil
	s.errs = nil
	s.dispatchPC = panics.Catcher{}
	s.producerPanic.Store(nil)
	s.ctx, s.cancel = context.WithCancelCause(s.parentCtx)
	s.waited = false
	s.once = sync.Once{}
}
