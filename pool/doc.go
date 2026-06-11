// Package pool provides a bounded, panic-safe task runner with support for
// recursive task submission.
//
// Tasks are spawned directly via [sync.WaitGroup.Go] without an internal task
// channel. A buffered channel semaphore bounds concurrency. This design allows
// running tasks to safely submit child tasks into the same pool — [Pool.Wait]
// waits for the transitive closure of all submitted work, including tasks
// submitted by tasks that were themselves submitted after Wait was called.
//
// [ResultPool] wraps a [Pool] for tasks that produce a typed result. Results
// are returned in submission order by default, and all results are included —
// even those from errored or panicking tasks — so nothing is silently dropped.
// Use [ResultPool.WithUnorderedResults] to skip the ordering step.
package pool
