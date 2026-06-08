package iter

import (
	"context"
	stditer "iter"
	"runtime"
	"sync"
)

// Option configures concurrent iteration behaviour.
type Option func(*opts)

type opts struct {
	maxGoroutines int
	ctx           context.Context
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
// interrupted.
func WithContext(ctx context.Context) Option {
	if ctx == nil {
		panic("iter: WithContext requires non-nil context")
	}
	return func(o *opts) { o.ctx = ctx }
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
