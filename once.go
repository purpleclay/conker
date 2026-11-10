package conker

import (
	"sync"

	"github.com/purpleclay/conker/panics"
)

// OnceErr returns a function that invokes fn at most once, no matter how many
// goroutines call the returned function, and how often. Every caller receives
// the same (T, error) pair produced by that single call.
//
// If fn panics, the panic is recovered and every caller, including the first,
// receives the zero value of T and the same [*panics.Recovered] as the error,
// which matches [panics.ErrPanic] through [errors.Is]. This is where it
// differs from [sync.OnceValues], which re-panics on every call.
//
// Example:
//
//	load := conker.OnceErr(func() (*Config, error) {
//	    return loadConfig("config.yaml")
//	})
//
//	cfg, err := load() // loads config.yaml
//	cfg, err = load()  // returns the cached result; fn is not called again
func OnceErr[T any](fn func() (T, error)) func() (T, error) {
	var (
		once sync.Once
		val  T
		err  error
	)
	return func() (T, error) {
		once.Do(func() {
			var pc panics.Catcher
			pc.Try(func() { val, err = fn() })
			if r := pc.Recovered(); r != nil {
				var zero T
				val, err = zero, r
			}
		})
		return val, err
	}
}
