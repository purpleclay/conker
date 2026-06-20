package stream

import (
	"context"
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
// Stream is single-shot: [Stream.Wait] is terminal, and [Stream.Go] /
// [Stream.GoCtx] panic if called after Wait has been called. Unlike
// [pool.Pool], callbacks must not submit further work — Stream has no
// recursive submission support. Use [pool.Pool] for workloads where tasks
// need to submit child tasks.
//
// Example:
//
//	s := stream.New().WithMaxGoroutines(4)
//	for _, item := range items {
//	    s.Go(func(ctx context.Context) stream.Callback {
//	        result := process(ctx, item)  // runs concurrently
//	        return func() {
//	            emit(result)  // runs in submission order
//	        }
//	    })
//	}
//	s.Wait()
type Stream struct {
	sem    chan struct{}
	ctx    context.Context
	cancel context.CancelCauseFunc

	// submitted carries per-producer result channels in submission order.
	// The dispatcher reads them in order and blocks on each until the producer
	// writes its result.
	submitted    chan chan callbackOrPanic
	dispatchDone chan struct{}

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

	// cfgMu guards sem, started, and waited, preventing reconfiguration after
	// the first Go call and making repeated Wait calls a no-op.
	cfgMu   sync.Mutex
	started bool
	waited  bool

	once sync.Once
}

// New returns a Stream with a default concurrency of [runtime.GOMAXPROCS](0).
func New() *Stream {
	ctx, cancel := context.WithCancelCause(context.Background())
	return &Stream{
		sem:    make(chan struct{}, runtime.GOMAXPROCS(0)),
		ctx:    ctx,
		cancel: cancel,
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
// [Stream.Wait] — Stream is single-shot; use [New] for a fresh one, or
// [pool.Pool] for workloads where tasks submit further work.
func (s *Stream) GoCtx(ctx context.Context, fn func(context.Context) Callback) error {
	s.cfgMu.Lock()
	if s.sem == nil {
		s.cfgMu.Unlock()
		panic("stream: use New() before calling GoCtx")
	}
	if s.waited {
		s.cfgMu.Unlock()
		panic("stream: Go called after Wait — Stream is single-shot")
	}
	s.started = true
	s.submitWg.Add(1)
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
		var cb Callback
		pc.Try(func() { cb = fn(s.ctx) })

		if r := pc.Recovered(); r != nil {
			slot <- callbackOrPanic{panic: r}
		} else {
			slot <- callbackOrPanic{cb: cb}
		}
	}()
	return nil
}

// Go submits fn as a producer. It blocks until a goroutine slot is available.
// The producer receives the stream's internal context.
//
// Go panics if called on a zero-value Stream, or if called after
// [Stream.Wait] — Stream is single-shot; use [New] for a fresh one, or
// [pool.Pool] for workloads where tasks submit further work.
func (s *Stream) Go(fn func(context.Context) Callback) {
	_ = s.GoCtx(s.ctx, fn)
}

// Wait blocks until all producers have finished and all callbacks have run in
// submission order. Panics from producers or callbacks are captured and
// re-panicked as *[panics.Recovered] values.
//
// A panic in any producer or callback halts the dispatcher immediately. All
// pending callbacks — including those from producers that have already
// completed — are silently dropped. This differs from [pool.Pool], which
// collects every error and continues. The difference is intentional: because
// callbacks must fire in submission order, there is no meaningful way to skip
// a failed slot and resume the sequence.
//
// Wait is terminal: once it returns, the Stream is done and [Stream.Go] /
// [Stream.GoCtx] panic on any further call, including one made from a
// callback. Stream has no recursive submission support — use [pool.Pool] for
// workloads where tasks need to submit child tasks. A second call to Wait is
// a no-op. Wait is also a no-op when no tasks have been submitted.
func (s *Stream) Wait() {
	s.cfgMu.Lock()
	if s.waited {
		s.cfgMu.Unlock()
		return
	}
	s.waited = true
	s.cfgMu.Unlock()

	// No GoCtx call admitted before the flip above can still be sending into
	// submitted once this returns, so closing it below is race-free.
	s.submitWg.Wait()

	defer s.producerWg.Wait()

	if s.submitted == nil {
		s.cancel(nil)
		return
	}
	close(s.submitted)
	<-s.dispatchDone
	s.cancel(nil)

	// Producer panic takes priority: it has the original stack trace intact.
	if r := s.producerPanic.Load(); r != nil {
		panic(r)
	}
	s.dispatchPC.Repanic()
}
