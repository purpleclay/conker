package conker

import "sync"

// OnceErr returns a function that invokes fn at most once, no matter how many
// goroutines call the returned function, and how often. Every caller receives
// the same (T, error) pair produced by that single call.
//
// It is the error-returning equivalent of [sync.OnceValue].
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
		once.Do(func() { val, err = fn() })
		return val, err
	}
}
