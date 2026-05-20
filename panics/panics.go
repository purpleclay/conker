package panics

import (
	"errors"
	"fmt"
	"runtime/debug"
	"sync/atomic"
)

// ErrPanic is the sentinel matched by [errors.Is] for any recovered panic.
//
// Callers that only need to detect "was this a panic?" can check against
// ErrPanic without importing or naming the [*Recovered] type directly:
//
//	if errors.Is(err, panics.ErrPanic) {
//	    log.Println("a goroutine panicked:", err)
//	}
var ErrPanic = errors.New("recovered panic")

// Recovered is a captured panic. It implements error and integrates with the
// standard errors package:
//
//   - [errors.Is](r, [ErrPanic]) returns true for any Recovered value.
//   - [errors.As] walks through to the original error when the panic value was
//     itself an error (for example, panic(fmt.Errorf("...: %w", sentinel))).
//
// Stack is captured at the moment of panic, not at the point [Catcher.Repanic]
// is called, so the trace points directly at the offending code rather than at
// the recovery site.
//
// Example — matching a domain sentinel through a recovered panic:
//
//	var ErrTimeout = errors.New("timeout")
//
//	var c panics.Catcher
//	c.Try(func() {
//	    panic(fmt.Errorf("request failed: %w", ErrTimeout))
//	})
//
//	r := c.Recovered()
//	fmt.Println(errors.Is(r, panics.ErrPanic)) // true
//	fmt.Println(errors.Is(r, ErrTimeout))      // true — Unwrap walks the chain
//
//nolint:errname
type Recovered struct {
	// Value is the original value passed to panic.
	Value any

	// Stack is the goroutine stack captured at the panic site via debug.Stack.
	Stack []byte
}

// Error implements the error interface, returning the panic value and the
// captured stack trace.
func (r *Recovered) Error() string {
	if r == nil {
		return "panic: <nil>"
	}
	return fmt.Sprintf("panic: %v\n\n%s", r.Value, r.Stack)
}

// Is reports whether this Recovered matches target. It returns true when
// target is [ErrPanic], enabling [errors.Is] without a type assertion.
func (r *Recovered) Is(target error) bool {
	return r != nil && target == ErrPanic
}

// Unwrap returns the underlying error when Value was itself an error, enabling
// [errors.As] to reach through Recovered to the original type. Returns nil
// when the panic value was not an error (for example, panic("string value")).
func (r *Recovered) Unwrap() error {
	if r == nil {
		return nil
	}
	if err, ok := r.Value.(error); ok {
		return err
	}
	return nil
}

// Catcher captures the first panic from one or more concurrent goroutines.
// The zero value is ready to use without initialisation.
//
// Catcher applies first-panic-wins semantics via a lock-free CAS on an
// [atomic.Pointer]: when multiple goroutines panic concurrently, only the
// first Recovered is stored and subsequent panics are discarded. This is the
// correct behaviour for WaitGroup, which re-panics with a single value on
// Wait.
//
// For independent tasks where every panic matters, use one Catcher per
// goroutine. pool.Pool does this automatically, promoting each per-task panic
// to an error in its collected error slice so no panic is silently lost.
//
// Example — basic recovery:
//
//	var c panics.Catcher
//	c.Try(func() {
//	    panic("something went wrong")
//	})
//
//	if r := c.Recovered(); r != nil {
//	    fmt.Println("caught:", r.Value)
//	    fmt.Println(errors.Is(r, panics.ErrPanic)) // true
//	}
type Catcher struct {
	recovered atomic.Pointer[Recovered]
}

// Try calls f and recovers any panic it raises. Safe to call concurrently
// from multiple goroutines; only the first panic is retained.
func (c *Catcher) Try(f func()) {
	defer c.tryRecover()
	f()
}

func (c *Catcher) tryRecover() {
	if v := recover(); v != nil {
		c.recovered.CompareAndSwap(nil, &Recovered{
			Value: v,
			Stack: debug.Stack(),
		})
	}
}

// Recovered returns the first panic captured by [Catcher.Try], or nil if no
// panic occurred.
func (c *Catcher) Recovered() *Recovered { return c.recovered.Load() }

// Repanic re-panics with the captured [Recovered] value. It is a no-op when
// no panic was captured. Because the stack trace inside Recovered was captured
// at the original panic site, the trace still points at the offending code
// even after the panic has travelled through Repanic.
//
// Example — propagating a recovered panic up the call stack:
//
//	func process() {
//	    var c panics.Catcher
//	    c.Try(riskyOperation)
//	    if r := c.Recovered(); r != nil {
//	        log.Printf("operation panicked at:\n%s", r.Stack)
//	    }
//	    c.Repanic() // re-raises if a panic was captured, no-op otherwise
//	}
func (c *Catcher) Repanic() {
	if r := c.Recovered(); r != nil {
		panic(r)
	}
}
