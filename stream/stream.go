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
	}()
}

// GoCtx submits fn as a producer, returning [context.Err] if ctx is cancelled
// while waiting for a goroutine slot. It returns nil on successful submission.
//
// The producer receives the stream's internal context, not the caller's ctx.
// The caller's ctx is used only to unblock the submission wait under
// backpressure.
//
// GoCtx panics if called on a zero-value Stream; use [New] before submitting.
func (s *Stream) GoCtx(ctx context.Context, fn func(context.Context) Callback) error {
	s.cfgMu.Lock()
	if s.sem == nil {
		s.cfgMu.Unlock()
		panic("stream: use New() before calling GoCtx")
	}
	s.started = true
	s.cfgMu.Unlock()

	s.once.Do(s.start)

	select {
	case s.sem <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}

	slot := make(chan callbackOrPanic, 1)
	s.submitted <- slot

	go func() {
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
// Go panics if called on a zero-value Stream; use [New] before submitting.
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
// Wait is a no-op when no tasks have been submitted.
func (s *Stream) Wait() {
	s.cfgMu.Lock()
	if s.waited {
		s.cfgMu.Unlock()
		return
	}
	s.waited = true
	s.cfgMu.Unlock()

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
