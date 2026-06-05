package conker

import (
	"sync"

	"github.com/purpleclay/conker/panics"
)

// WaitGroup is a panic-safe wrapper around [sync.WaitGroup] that propagates
// panics from goroutines spawned with [WaitGroup.Go] to the caller of
// [WaitGroup.Wait] or [WaitGroup.WaitAndRecover].
//
// The zero value is ready to use. WaitGroup is reusable across multiple
// Wait cycles when no panic occurs. If a goroutine panics, create a new
// WaitGroup for subsequent work rather than reusing the same instance.
//
// Example — basic usage:
//
//	var wg conker.WaitGroup
//	wg.Go(func() { doWork() })
//	wg.Go(func() { doMoreWork() })
//	wg.Wait() // re-panics if any goroutine panicked
//
// Example — recovering from a panic without propagating:
//
//	var wg conker.WaitGroup
//	wg.Go(func() { riskyWork() })
//	if r := wg.WaitAndRecover(); r != nil {
//	    log.Printf("goroutine panicked at:\n%s", r.Stack)
//	}
type WaitGroup struct {
	wg sync.WaitGroup
	pc panics.Catcher
}

// Go spawns f in a new goroutine. Any panic raised by f is captured and
// surfaced when [WaitGroup.Wait] or [WaitGroup.WaitAndRecover] is called,
// with the stack trace preserved at the original panic site.
func (w *WaitGroup) Go(f func()) {
	w.wg.Go(func() { w.pc.Try(f) })
}

// Wait blocks until all goroutines launched with [WaitGroup.Go] have returned,
// then re-panics with the first captured panic as a *[panics.Recovered]. It is
// a no-op if no goroutine panicked.
func (w *WaitGroup) Wait() {
	w.wg.Wait()
	w.pc.Repanic()
}

// WaitAndRecover blocks until all goroutines launched with [WaitGroup.Go] have
// returned and returns the first captured panic, or nil if none occurred.
func (w *WaitGroup) WaitAndRecover() *panics.Recovered {
	w.wg.Wait()
	return w.pc.Recovered()
}
