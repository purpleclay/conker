package iter

import (
	"context"
	"errors"
	"fmt"
	stditer "iter"
	"maps"
	"runtime"
	"slices"
	"sync"

	"github.com/purpleclay/conker/panics"
)

// Option configures concurrent iteration behaviour.
type Option func(*opts)

type opts struct {
	maxGoroutines int
	ctx           context.Context
	cancelOnError bool
}

// WithMaxGoroutines sets the maximum number of goroutines that may process
// elements concurrently. It panics if n ≤ 0.
func WithMaxGoroutines(n int) Option {
	if n <= 0 {
		panic("iter: WithMaxGoroutines requires n > 0")
	}
	return func(o *opts) { o.maxGoroutines = n }
}

// WithContext sets the context governing this iteration. When the context is
// cancelled, no further elements are dispatched; in-flight goroutines are not
// interrupted. In the functions whose fn takes a context — those ending in Ctx
// or Err — cancellation also propagates into in-flight fn calls via the
// context they receive.
func WithContext(ctx context.Context) Option {
	if ctx == nil {
		panic("iter: WithContext requires non-nil context")
	}
	return func(o *opts) { o.ctx = ctx }
}

// WithCancelOnError stops dispatching new elements and cancels the context
// passed to in-flight fn calls as soon as any fn call returns a non-nil error.
// It only takes effect in the error-returning variants: [MapSeqErr],
// [ForEachSeqErr], [MapErr], and [ForEachErr].
func WithCancelOnError() Option {
	return func(o *opts) { o.cancelOnError = true }
}

func buildOpts(options []Option) opts {
	o := opts{ctx: context.Background()}
	for _, opt := range options {
		opt(&o)
	}
	if o.maxGoroutines == 0 {
		o.maxGoroutines = runtime.GOMAXPROCS(0)
	}
	return o
}

// mapSlot holds the result of one concurrent mapping goroutine and a channel
// closed when that goroutine finishes, allowing the consumer to wait for
// results in submission order.
type mapSlot[R any] struct {
	done chan struct{}
	val  R
}

// kvPair is an intermediate type used by [MapSeq2] to adapt a Seq2 into a
// Seq so it can be processed by [MapSeq].
type kvPair[K, V any] struct {
	k K
	v V
}

// ElemError associates an error with the zero-based index of the input
// element that produced it, in [MapSeqErr], [ForEachSeqErr], [MapErr], and
// [ForEachErr]. Use
// [errors.As] to recover the index of a failing element from the joined
// error either function returns.
//
// Example:
//
//	results, err := iter.MapSeqErr(in, fn)
//	var ee *iter.ElemError
//	if errors.As(err, &ee) {
//	    // results[ee.Index] holds the zero value of R.
//	}
type ElemError struct {
	// Index is the zero-based position of the input element that produced Err.
	Index int

	// Err is the error fn returned, or the panic recovered from it.
	Err error
}

// Error implements the error interface.
func (e *ElemError) Error() string {
	return fmt.Sprintf("element %d: %v", e.Index, e.Err)
}

// Unwrap returns the wrapped error, enabling [errors.Is] and [errors.As] to
// reach through ElemError to Err.
func (e *ElemError) Unwrap() error { return e.Err }

// MapSeq concurrently maps in using fn and returns a new [iter.Seq] that
// yields results in the same order as the input. Mapping work is lazy: it
// begins when the caller ranges over the returned Seq and stops when the
// range breaks or the context is cancelled.
//
// At most [WithMaxGoroutines] mapping goroutines run concurrently (default:
// [runtime.GOMAXPROCS](0)). Results are always yielded in the original
// sequence order, regardless of completion order.
//
// Cancelling the context provided via [WithContext] stops new elements from
// being dispatched; in-flight mapping goroutines are not interrupted.
// Use [MapSeqCtx] when fn needs to observe cancellation.
//
// Example:
//
//	doubled := iter.MapSeq(slices.Values(nums), func(n int) int { return n * 2 })
//	for v := range doubled {
//	    fmt.Println(v)
//	}
func MapSeq[T, R any](in stditer.Seq[T], fn func(T) R, options ...Option) stditer.Seq[R] {
	return mapSeq(in, fn, nil, options)
}

// MapSeqCtx is [MapSeq] with a context passed into each fn call, so in-flight
// work can observe cancellation rather than only stopping dispatch.
//
// The context is derived from the one provided via [WithContext] and is
// cancelled when that context is cancelled, when the caller breaks out of the
// range, or when any fn call panics. Breaking out of the range still waits
// for in-flight calls to return, so fn should observe ctx.Done() to return
// promptly.
//
// Example:
//
//	pages := iter.MapSeqCtx(slices.Values(urls), func(ctx context.Context, url string) []byte {
//	    return fetch(ctx, url)
//	}, iter.WithMaxGoroutines(8))
//	for page := range pages {
//	    if done(page) {
//	        break // cancels the context of every in-flight fetch
//	    }
//	}
func MapSeqCtx[T, R any](in stditer.Seq[T], fn func(context.Context, T) R, options ...Option) stditer.Seq[R] {
	return mapSeq(in, nil, fn, options)
}

// mapRun is the state shared by one mapSeq call's dispatcher and workers.
// Workers capture a single pointer to it rather than each field separately,
// keeping the per-element goroutine closure small.
type mapRun[T, R any] struct {
	fn     func(T) R
	fnCtx  func(context.Context, T) R
	ctx    context.Context
	cancel context.CancelFunc
	sem    chan struct{}
	pc     panics.Catcher
}

// call invokes whichever of fn and fnCtx is set, without an adapter closure,
// storing the result in s.
func (r *mapRun[T, R]) call(v T, s *mapSlot[R]) {
	if r.fnCtx != nil {
		s.val = r.fnCtx(r.ctx, v)
		return
	}
	s.val = r.fn(v)
}

// mapSeq implements [MapSeq] and [MapSeqCtx]; exactly one of fn and fnCtx is
// non-nil. With fn, no derived context is created: the governing context is
// used directly, keeping context.Background's nil Done channel, which select
// skips for free, on the context-free hot path.
func mapSeq[T, R any](in stditer.Seq[T], fn func(T) R, fnCtx func(context.Context, T) R, options []Option) stditer.Seq[R] {
	return func(yield func(R) bool) {
		o := buildOpts(options)

		r := &mapRun[T, R]{
			fn:     fn,
			fnCtx:  fnCtx,
			ctx:    o.ctx,
			cancel: func() {},
			sem:    make(chan struct{}, o.maxGoroutines),
		}
		if fnCtx != nil {
			r.ctx, r.cancel = context.WithCancel(o.ctx)
		}
		defer r.cancel()

		ctx, sem, pc := r.ctx, r.sem, &r.pc
		ordered := make(chan *mapSlot[R], o.maxGoroutines)
		done := make(chan struct{})

		go func() {
			defer close(ordered)
			// stopped returns true without blocking if the consumer has broken,
			// the context has been cancelled, or fn has panicked. Used before and
			// after acquiring the semaphore to prevent dispatching work when both
			// a free slot and a stop signal are ready (Go's select is otherwise
			// non-deterministic in that case).
			stopped := func() bool {
				select {
				case <-done:
					return true
				case <-ctx.Done():
					return true
				default:
					return pc.Recovered() != nil
				}
			}
			for v := range in {
				if stopped() {
					return
				}
				select {
				case sem <- struct{}{}:
				case <-done:
					return
				case <-ctx.Done():
					return
				}
				if stopped() {
					<-sem
					return
				}

				s := &mapSlot[R]{done: make(chan struct{})}

				// Push in submission order. If the consumer has already broken,
				// release the semaphore slot and exit rather than blocking forever.
				select {
				case ordered <- s:
				case <-done:
					<-sem
					return
				}

				go func(r *mapRun[T, R], v T, s *mapSlot[R]) {
					defer func() { <-r.sem; close(s.done) }()
					r.pc.Try(func() { r.call(v, s) })
					if r.pc.Recovered() != nil {
						r.cancel() // stop in-flight siblings as well as dispatch
					}
				}(r, v, s)
			}
		}()

		// panicked tracks whether fn has panicked so done is closed at most
		// once, and so remaining slots are drained (every in-flight goroutine
		// joined) without yielding, before re-panicking in this goroutine.
		var panicked bool
		for s := range ordered {
			<-s.done
			if !panicked && pc.Recovered() != nil {
				panicked = true
				close(done)
			}
			if panicked {
				continue
			}
			if !yield(s.val) {
				close(done)
				r.cancel() // reach in-flight fn calls before draining them
				// Drain in-flight work so late panics are observed in this goroutine.
				for s := range ordered {
					<-s.done
				}
				pc.Repanic()
				return
			}
		}
		pc.Repanic()
	}
}

// MapSeq2 concurrently maps the key-value pairs from in using fn and returns
// a new [iter.Seq] that yields results in the same order as the input.
// Mapping work is lazy: it begins when the caller ranges over the returned
// Seq and stops when the range breaks or the context is cancelled.
//
// At most [WithMaxGoroutines] mapping goroutines run concurrently (default:
// [runtime.GOMAXPROCS](0)). Results are always yielded in the original
// sequence order, regardless of completion order.
//
// Cancelling the context provided via [WithContext] stops new elements from
// being dispatched; in-flight mapping goroutines are not interrupted.
// Use [MapSeq2Ctx] when fn needs to observe cancellation.
//
// Example:
//
//	sizes := iter.MapSeq2(maps.All(m), func(k string, v []byte) int { return len(v) })
//	for size := range sizes {
//	    fmt.Println(size)
//	}
func MapSeq2[K, V, R any](in stditer.Seq2[K, V], fn func(K, V) R, options ...Option) stditer.Seq[R] {
	return MapSeq(
		func(yield func(kvPair[K, V]) bool) {
			for k, v := range in {
				if !yield(kvPair[K, V]{k, v}) {
					return
				}
			}
		},
		func(p kvPair[K, V]) R { return fn(p.k, p.v) },
		options...,
	)
}

// MapSeq2Ctx is [MapSeq2] with a context passed into each fn call. The
// context is cancelled on the same conditions as [MapSeqCtx]: when the
// [WithContext] context is cancelled, the caller breaks out of the range, or
// any fn call panics.
func MapSeq2Ctx[K, V, R any](in stditer.Seq2[K, V], fn func(context.Context, K, V) R, options ...Option) stditer.Seq[R] {
	return MapSeqCtx(
		func(yield func(kvPair[K, V]) bool) {
			for k, v := range in {
				if !yield(kvPair[K, V]{k, v}) {
					return
				}
			}
		},
		func(ctx context.Context, p kvPair[K, V]) R { return fn(ctx, p.k, p.v) },
		options...,
	)
}

// ForEachSeq concurrently calls fn for each element in in. It blocks until
// every dispatched element has been processed.
//
// At most [WithMaxGoroutines] goroutines run concurrently (default:
// [runtime.GOMAXPROCS](0)).
//
// Cancelling the context provided via [WithContext] stops new elements from
// being dispatched; in-flight goroutines are not interrupted. Elements not yet
// dispatched are skipped.
// Use [ForEachSeqCtx] when fn needs to observe cancellation.
//
// Example:
//
//	iter.ForEachSeq(slices.Values(items), func(item Item) {
//	    process(item)
//	}, iter.WithMaxGoroutines(8))
func ForEachSeq[T any](in stditer.Seq[T], fn func(T), options ...Option) {
	forEachSeq(in, fn, nil, options)
}

// ForEachSeqCtx is [ForEachSeq] with a context passed into each fn call, so
// in-flight work can observe cancellation rather than only stopping dispatch.
// The context is derived from the one provided via [WithContext] and is
// cancelled when that context is cancelled or when any fn call panics.
//
// Example:
//
//	iter.ForEachSeqCtx(slices.Values(items), func(ctx context.Context, item Item) {
//	    process(ctx, item)
//	}, iter.WithContext(ctx), iter.WithMaxGoroutines(8))
func ForEachSeqCtx[T any](in stditer.Seq[T], fn func(context.Context, T), options ...Option) {
	forEachSeq(in, nil, fn, options)
}

// forEachRun is the state shared by one forEachSeq call's dispatcher and
// workers; see mapRun.
type forEachRun[T any] struct {
	fn     func(T)
	fnCtx  func(context.Context, T)
	ctx    context.Context
	cancel context.CancelFunc
	sem    chan struct{}
	pc     panics.Catcher
}

// call invokes whichever of fn and fnCtx is set, without an adapter closure.
func (r *forEachRun[T]) call(v T) {
	if r.fnCtx != nil {
		r.fnCtx(r.ctx, v)
		return
	}
	r.fn(v)
}

// forEachSeq implements [ForEachSeq] and [ForEachSeqCtx]; exactly one of fn
// and fnCtx is non-nil. See mapSeq for why the context-free path skips
// deriving a cancellable context.
func forEachSeq[T any](in stditer.Seq[T], fn func(T), fnCtx func(context.Context, T), options []Option) {
	o := buildOpts(options)

	r := &forEachRun[T]{
		fn:     fn,
		fnCtx:  fnCtx,
		ctx:    o.ctx,
		cancel: func() {},
		sem:    make(chan struct{}, o.maxGoroutines),
	}
	if fnCtx != nil {
		r.ctx, r.cancel = context.WithCancel(o.ctx)
	}
	defer r.cancel()

	ctx, sem, pc := r.ctx, r.sem, &r.pc
	var wg sync.WaitGroup

	// stopped returns true without blocking if the context has been cancelled
	// or fn has panicked. Used before and after acquiring the semaphore to
	// prevent dispatching work when both a free slot and a stop signal are
	// ready simultaneously.
	stopped := func() bool {
		select {
		case <-ctx.Done():
			return true
		default:
			return pc.Recovered() != nil
		}
	}

outer:
	for v := range in {
		if stopped() {
			break outer
		}
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			break outer
		}
		if stopped() {
			<-sem
			break outer
		}
		wg.Go(func() {
			defer func() { <-r.sem }()
			r.pc.Try(func() { r.call(v) })
			if r.pc.Recovered() != nil {
				r.cancel() // stop in-flight siblings as well as dispatch
			}
		})
	}
	wg.Wait()
	pc.Repanic()
}

// MapMap concurrently maps the key-value pairs of in using fn and returns a
// slice containing one result per pair. Results are not returned in any
// defined order — Go's map iteration order is intentionally non-deterministic.
//
// At most [WithMaxGoroutines] mapping goroutines run concurrently (default:
// [runtime.GOMAXPROCS](0)).
//
// Cancelling the context provided via [WithContext] stops new elements from
// being dispatched; in-flight mapping goroutines are not interrupted.
// Use [MapMapCtx] when fn needs to observe cancellation.
//
// Example:
//
//	counts := iter.MapMap(pages, func(url string, body []byte) int { return len(body) })
func MapMap[K comparable, V, R any](in map[K]V, fn func(K, V) R, options ...Option) []R {
	return slices.Collect(MapSeq2(maps.All(in), fn, options...))
}

// ForEachMap concurrently calls fn for each key-value pair in in. It blocks
// until all pairs have been processed. Pairs are visited in non-deterministic
// order — this matches Go's map iteration semantics.
//
// At most [WithMaxGoroutines] goroutines run concurrently (default:
// [runtime.GOMAXPROCS](0)).
//
// Cancelling the context provided via [WithContext] stops new elements from
// being dispatched; in-flight goroutines are not interrupted.
// Use [ForEachMapCtx] when fn needs to observe cancellation.
//
// Example:
//
//	iter.ForEachMap(headers, func(k, v string) {
//	    log.Printf("%s: %s", k, v)
//	}, iter.WithMaxGoroutines(8))
func ForEachMap[K comparable, V any](in map[K]V, fn func(K, V), options ...Option) {
	ForEachSeq(func(yield func(kvPair[K, V]) bool) {
		for k, v := range in {
			if !yield(kvPair[K, V]{k, v}) {
				return
			}
		}
	}, func(p kvPair[K, V]) { fn(p.k, p.v) }, options...)
}

// MapMapCtx is [MapMap] with a context passed into each fn call. The context
// is cancelled on the same conditions as [MapSeqCtx]: when the [WithContext]
// context is cancelled or any fn call panics.
func MapMapCtx[K comparable, V, R any](in map[K]V, fn func(context.Context, K, V) R, options ...Option) []R {
	return slices.Collect(MapSeq2Ctx(maps.All(in), fn, options...))
}

// ForEachMapCtx is [ForEachMap] with a context passed into each fn call. The
// context is cancelled on the same conditions as [ForEachSeqCtx]: when the
// [WithContext] context is cancelled or any fn call panics.
func ForEachMapCtx[K comparable, V any](in map[K]V, fn func(context.Context, K, V), options ...Option) {
	ForEachSeqCtx(func(yield func(kvPair[K, V]) bool) {
		for k, v := range in {
			if !yield(kvPair[K, V]{k, v}) {
				return
			}
		}
	}, func(ctx context.Context, p kvPair[K, V]) { fn(ctx, p.k, p.v) }, options...)
}

// MapSeqErr concurrently maps in using fn, passing a derived context into each
// call, and returns the results of every dispatched element in submission
// order alongside any joined errors. A result for an errored call holds the
// zero value of R. Each error is wrapped in an [ElemError] carrying the
// zero-based index of the element that produced it, so callers can identify
// which positions in the result slice are holes; use [errors.As] to recover
// it. Joined errors are ordered by index, regardless of completion order.
//
// At most [WithMaxGoroutines] goroutines run concurrently (default:
// [runtime.GOMAXPROCS](0)).
//
// The context each fn call receives is derived from the context provided via
// [WithContext]. Cancelling that context stops new elements from being
// dispatched and propagates into in-flight fn calls via their context argument.
//
// [WithCancelOnError] cancels the context passed to all in-flight fn calls as
// soon as any call returns a non-nil error, and stops further dispatch.
//
// When dispatch stops early for either reason, the result holds only the
// dispatched elements, in order. Skipped elements are not reported as errors;
// check the context's Err to detect cancellation.
//
// Example:
//
//	pages, err := iter.MapSeqErr(slices.Values(urls), func(ctx context.Context, url string) ([]byte, error) {
//	    return fetch(ctx, url)
//	}, iter.WithMaxGoroutines(8))
func MapSeqErr[T, R any](in stditer.Seq[T], fn func(context.Context, T) (R, error), options ...Option) ([]R, error) {
	o := buildOpts(options)

	ctx, cancel := context.WithCancel(o.ctx)
	defer cancel()

	ordered := make(chan *mapSlot[R], o.maxGoroutines)
	sem := make(chan struct{}, o.maxGoroutines)
	var mu sync.Mutex
	var errs []*ElemError

	go func() {
		defer close(ordered)
		stopped := func() bool {
			select {
			case <-ctx.Done():
				return true
			default:
				return false
			}
		}
		var i int
		for v := range in {
			idx := i
			i++
			if stopped() {
				return
			}
			select {
			case sem <- struct{}{}:
			case <-ctx.Done():
				return
			}
			if stopped() {
				<-sem
				return
			}
			s := &mapSlot[R]{done: make(chan struct{})}
			select {
			case ordered <- s:
			case <-ctx.Done():
				<-sem
				return
			}
			go func(idx int, v T, s *mapSlot[R]) {
				defer func() { <-sem; close(s.done) }()
				var pc panics.Catcher
				var r R
				var err error
				pc.Try(func() { r, err = fn(ctx, v) })
				if rec := pc.Recovered(); rec != nil {
					err = rec
				}
				if err != nil {
					mu.Lock()
					errs = append(errs, &ElemError{Index: idx, Err: err})
					mu.Unlock()
					if o.cancelOnError {
						cancel()
					}
					return
				}
				s.val = r
			}(idx, v, s)
		}
	}()

	var out []R
	for s := range ordered {
		<-s.done
		out = append(out, s.val)
	}

	slices.SortFunc(errs, func(a, b *ElemError) int { return a.Index - b.Index })
	joined := make([]error, len(errs))
	for i, e := range errs {
		joined[i] = e
	}
	return out, errors.Join(joined...)
}

// ForEachSeqErr concurrently calls fn for each element in in, passing a
// derived context into each call. It blocks until every dispatched element
// has been processed and returns any joined errors. Each error is wrapped in an
// [ElemError] carrying the zero-based index of the element that produced it;
// use [errors.As] to recover it. Joined errors are ordered by index,
// regardless of completion order.
//
// At most [WithMaxGoroutines] goroutines run concurrently (default:
// [runtime.GOMAXPROCS](0)).
//
// The context each fn call receives is derived from the context provided via
// [WithContext]. Cancelling that context stops new elements from being
// dispatched and propagates into in-flight fn calls via their context argument.
//
// [WithCancelOnError] cancels the context passed to all in-flight fn calls as
// soon as any call returns a non-nil error, and stops further dispatch.
//
// Elements skipped when dispatch stops early for either reason are not
// reported as errors; check the context's Err to detect cancellation.
//
// Example:
//
//	err := iter.ForEachSeqErr(slices.Values(items), func(ctx context.Context, item Item) error {
//	    return process(ctx, item)
//	}, iter.WithMaxGoroutines(8))
func ForEachSeqErr[T any](in stditer.Seq[T], fn func(context.Context, T) error, options ...Option) error {
	o := buildOpts(options)

	ctx, cancel := context.WithCancel(o.ctx)
	defer cancel()

	sem := make(chan struct{}, o.maxGoroutines)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var errs []*ElemError

	stopped := func() bool {
		select {
		case <-ctx.Done():
			return true
		default:
			return false
		}
	}

	var i int
outer:
	for v := range in {
		idx := i
		i++
		if stopped() {
			break outer
		}
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			break outer
		}
		if stopped() {
			<-sem
			break outer
		}
		wg.Go(func() {
			defer func() { <-sem }()
			var pc panics.Catcher
			var err error
			pc.Try(func() { err = fn(ctx, v) })
			if r := pc.Recovered(); r != nil {
				err = r
			}
			if err != nil {
				mu.Lock()
				errs = append(errs, &ElemError{Index: idx, Err: err})
				mu.Unlock()
				if o.cancelOnError {
					cancel()
				}
			}
		})
	}
	wg.Wait()

	slices.SortFunc(errs, func(a, b *ElemError) int { return a.Index - b.Index })
	joined := make([]error, len(errs))
	for i, e := range errs {
		joined[i] = e
	}
	return errors.Join(joined...)
}

// Map concurrently maps in using fn and returns the results in the same order
// as the input. It is the slice form of [MapSeq]: options, ordering, and panic
// behaviour are identical. The output slice is allocated with capacity len(in)
// when the first result arrives; Map returns nil if no result is produced.
//
// Cancelling the context provided via [WithContext] stops new elements from
// being dispatched; in-flight mapping goroutines are not interrupted. The
// result then holds only the results of dispatched elements, in order.
// Use [MapCtx] when fn needs to observe cancellation.
//
// Example:
//
//	doubled := iter.Map(nums, func(n int) int { return n * 2 })
func Map[T, R any](in []T, fn func(T) R, options ...Option) []R {
	return collectN(len(in), MapSeq(slices.Values(in), fn, options...))
}

// MapCtx is [Map] with a context passed into each fn call. The context is
// cancelled on the same conditions as [MapSeqCtx]: when the [WithContext]
// context is cancelled or any fn call panics. Allocation and truncation
// behave as for Map.
//
// Example:
//
//	pages := iter.MapCtx(urls, func(ctx context.Context, url string) []byte {
//	    return fetch(ctx, url)
//	}, iter.WithContext(ctx), iter.WithMaxGoroutines(8))
func MapCtx[T, R any](in []T, fn func(context.Context, T) R, options ...Option) []R {
	return collectN(len(in), MapSeqCtx(slices.Values(in), fn, options...))
}

// collectN collects seq into a slice with capacity n. The slice is allocated
// on the first element, not up front, so a context cancelled before any
// dispatch costs nothing; collectN returns nil if seq yields nothing.
func collectN[R any](n int, seq stditer.Seq[R]) []R {
	var out []R
	for r := range seq {
		if out == nil {
			out = make([]R, 0, n)
		}
		out = append(out, r)
	}
	return out
}

// MapErr concurrently maps in using fn, passing a derived context into each
// call, and returns the results of every dispatched element in input order
// alongside any joined errors. It is the slice form of [MapSeqErr]: options,
// ordering, [ElemError] indexing, and context behaviour are identical. A
// result for an errored element holds the zero value of R.
//
// Dispatch stops early if the context provided via [WithContext] is
// cancelled, or if [WithCancelOnError] is set and a call fails. The result
// then holds only the dispatched elements, in order. Skipped elements are not
// reported as errors; check the context's Err to detect cancellation.
//
// Example:
//
//	pages, err := iter.MapErr(urls, func(ctx context.Context, url string) ([]byte, error) {
//	    return fetch(ctx, url)
//	}, iter.WithMaxGoroutines(8))
func MapErr[T, R any](in []T, fn func(context.Context, T) (R, error), options ...Option) ([]R, error) {
	return MapSeqErr(slices.Values(in), fn, options...)
}

// ForEach concurrently calls fn for each element in in. It blocks until every
// dispatched element has been processed. It is the slice form of
// [ForEachSeq]: options and panic behaviour are identical.
//
// Cancelling the context provided via [WithContext] stops new elements from
// being dispatched; in-flight goroutines are not interrupted. Elements not yet
// dispatched are skipped.
// Use [ForEachCtx] when fn needs to observe cancellation.
//
// Example:
//
//	iter.ForEach(items, func(item Item) {
//	    process(item)
//	}, iter.WithMaxGoroutines(8))
func ForEach[T any](in []T, fn func(T), options ...Option) {
	ForEachSeq(slices.Values(in), fn, options...)
}

// ForEachCtx is [ForEach] with a context passed into each fn call. The
// context is cancelled on the same conditions as [ForEachSeqCtx]: when the
// [WithContext] context is cancelled or any fn call panics.
//
// Example:
//
//	iter.ForEachCtx(items, func(ctx context.Context, item Item) {
//	    process(ctx, item)
//	}, iter.WithContext(ctx), iter.WithMaxGoroutines(8))
func ForEachCtx[T any](in []T, fn func(context.Context, T), options ...Option) {
	ForEachSeqCtx(slices.Values(in), fn, options...)
}

// ForEachErr concurrently calls fn for each element in in, passing a derived
// context into each call. It blocks until every dispatched element has been
// processed and returns any joined errors. It is the slice form of
// [ForEachSeqErr]: options, [ElemError] indexing, and context behaviour are
// identical.
//
// Dispatch stops early if the context provided via [WithContext] is
// cancelled, or if [WithCancelOnError] is set and a call fails. Skipped
// elements are not reported as errors; check the context's Err to detect
// cancellation.
//
// Example:
//
//	err := iter.ForEachErr(items, func(ctx context.Context, item Item) error {
//	    return process(ctx, item)
//	}, iter.WithMaxGoroutines(8))
func ForEachErr[T any](in []T, fn func(context.Context, T) error, options ...Option) error {
	return ForEachSeqErr(slices.Values(in), fn, options...)
}
