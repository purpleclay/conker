// Package pool provides a bounded, panic-safe task runner with support for
// recursive task submission.
//
// Tasks are spawned directly via [sync.WaitGroup.Go] without an internal task
// channel. A buffered channel semaphore bounds concurrency. This design allows
// running tasks to safely submit child tasks into the same pool — [Pool.Wait]
// waits for the transitive closure of all submitted work, including tasks
// submitted by tasks that were themselves submitted after Wait was called.
//
// [Pool.Go] blocks until a slot is free, [Pool.GoCtx] gives up if its context
// is cancelled first, and [Pool.TryGo] never blocks, reporting whether the
// task was accepted. Use TryGo from within a task to avoid deadlocking a small
// pool on recursive submission.
//
// Every task error is collected and [Pool.Wait] returns them joined via
// [errors.Join]. Panics do not propagate: each is converted to a
// *[panics.Recovered] error and collected like any other. For errgroup-style
// fail-fast behaviour, [Pool.WithCancelOnError] cancels every task's context
// on the first failure and [Pool.WithFirstError] makes Wait return only that
// error. [Pool.Reset] readies the pool for reuse.
//
// [ResultPool] wraps a [Pool] for tasks that produce a typed result. Results
// are returned in submission order by default, and all results are included —
// even those from errored or panicking tasks — so nothing is silently dropped.
// Use [ResultPool.WithUnorderedResults] to skip the ordering step.
package pool
