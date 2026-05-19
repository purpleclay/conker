// Package panics provides typed panic recovery that integrates with Go's
// standard errors package.
//
// The central type is [Recovered], a concrete error that wraps a panic value
// together with the stack trace captured at the panic site. Because Recovered
// implements error, it composes naturally with [errors.Is] and [errors.As]:
// callers can test whether any error is a recovered panic without importing
// this package's types, and can unwrap the original error when the panic value
// was itself an error.
//
// [Catcher] is the capture mechanism. Its zero value is ready to use. Multiple
// goroutines may call [Catcher.Try] concurrently; only the first panic is
// retained. This first-panic-wins design is intentional: it matches the
// WaitGroup and pool.Pool use cases where re-panicking with a single root
// cause is preferable to a flood of cascading panics from dependent goroutines.
//
// If you need every panic from a set of independent goroutines, give each its
// own Catcher and collect the results — that is exactly what pool.Pool does
// internally, converting each per-task panic into an error stored alongside
// normal task errors.
package panics
