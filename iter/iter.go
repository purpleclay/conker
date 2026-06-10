package iter

import (
	"context"
	"errors"
	stditer "iter"
	"maps"
	"runtime"
	"slices"
	"sync"
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
// interrupted. In the error-returning variants ([MapSeqErr], [ForEachSeqErr]),
// cancellation also propagates into in-flight fn calls via the context they
// receive.
func WithContext(ctx context.Context) Option {
	if ctx == nil {
		panic("iter: WithContext requires non-nil context")
	}
	return func(o *opts) { o.ctx = ctx }
}

// WithCancelOnError stops dispatching new elements and cancels the context
// passed to in-flight fn calls as soon as any fn call returns a non-nil error.
// It only takes effect in the error-returning variants: [MapSeqErr] and
// [ForEachSeqErr].
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
//
// Example:
//
//	doubled := iter.MapSeq(slices.Values(nums), func(n int) int { return n * 2 })
//	for v := range doubled {
//	    fmt.Println(v)
//	}
func MapSeq[T, R any](in stditer.Seq[T], fn func(T) R, options ...Option) stditer.Seq[R] {
	return func(yield func(R) bool) {
		o := buildOpts(options)

		ordered := make(chan *mapSlot[R], o.maxGoroutines)
		sem := make(chan struct{}, o.maxGoroutines)
		done := make(chan struct{})

		go func() {
			defer close(ordered)
			// stopped returns true without blocking if either the consumer has
			// broken or the context has been cancelled. Used before and after
			// acquiring the semaphore to prevent dispatching work when both a
			// free slot and a stop signal are ready (Go's select is otherwise
			// non-deterministic in that case).
			stopped := func() bool {
				select {
				case <-done:
					return true
				case <-o.ctx.Done():
					return true
				default:
					return false
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
				case <-o.ctx.Done():
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

				go func(v T, s *mapSlot[R]) {
					defer func() { <-sem; close(s.done) }()
					s.val = fn(v)
				}(v, s)
			}
		}()

		for s := range ordered {
			<-s.done
			if !yield(s.val) {
				close(done)
				return
			}
		}
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

// ForEachSeq concurrently calls fn for each element in in. It blocks until
// all elements have been processed.
//
// At most [WithMaxGoroutines] goroutines run concurrently (default:
// [runtime.GOMAXPROCS](0)).
//
// Cancelling the context provided via [WithContext] stops new elements from
// being dispatched; in-flight goroutines are not interrupted.
//
// Example:
//
//	iter.ForEachSeq(slices.Values(items), func(item Item) {
//	    process(item)
//	}, iter.WithMaxGoroutines(8))
func ForEachSeq[T any](in stditer.Seq[T], fn func(T), options ...Option) {
	o := buildOpts(options)
	sem := make(chan struct{}, o.maxGoroutines)
	var wg sync.WaitGroup

	// stopped returns true without blocking if the context has been cancelled.
	// Used before and after acquiring the semaphore to prevent dispatching work
	// when both a free slot and a stop signal are ready simultaneously.
	stopped := func() bool {
		select {
		case <-o.ctx.Done():
			return true
		default:
			return false
		}
	}

outer:
	for v := range in {
		if stopped() {
			break outer
		}
		select {
		case sem <- struct{}{}:
		case <-o.ctx.Done():
			break outer
		}
		if stopped() {
			<-sem
			break outer
		}
		wg.Go(func() {
			defer func() { <-sem }()
			fn(v)
		})
	}
	wg.Wait()
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

// MapSeqErr concurrently maps in using fn, passing a derived context into each
// call, and returns all results in submission order alongside any joined errors.
// Results are collected for every element — a result for an errored call holds
// the zero value of R.
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
	var errs []error

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
		for v := range in {
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
			go func(v T, s *mapSlot[R]) {
				defer func() { <-sem; close(s.done) }()
				r, err := fn(ctx, v)
				if err != nil {
					mu.Lock()
					errs = append(errs, err)
					mu.Unlock()
					if o.cancelOnError {
						cancel()
					}
					return
				}
				s.val = r
			}(v, s)
		}
	}()

	var out []R
	for s := range ordered {
		<-s.done
		out = append(out, s.val)
	}
	return out, errors.Join(errs...)
}

// ForEachSeqErr concurrently calls fn for each element in in, passing a
// derived context into each call. It blocks until all elements have been
// processed and returns any joined errors.
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
	var errs []error

	stopped := func() bool {
		select {
		case <-ctx.Done():
			return true
		default:
			return false
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
			defer func() { <-sem }()
			if err := fn(ctx, v); err != nil {
				mu.Lock()
				errs = append(errs, err)
				mu.Unlock()
				if o.cancelOnError {
					cancel()
				}
			}
		})
	}
	wg.Wait()
	return errors.Join(errs...)
}
