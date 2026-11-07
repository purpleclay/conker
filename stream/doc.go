// Package stream provides ordered concurrent processing: producer functions
// run concurrently, but their returned callbacks execute serially in
// submission order.
//
// Create a Stream with [New] and submit producers with [Stream.Go]. Call
// [Stream.Wait] to block until all producers have finished and all callbacks
// have run. Call [Stream.Reset] to reuse the Stream for another batch.
//
// Producers fail by returning an error. The failed producer's callback is
// skipped, the remaining callbacks still run in order, and Wait returns the
// errors joined in submission order. [Stream.WithCancelOnError] cancels the
// remaining producers on the first error, and [Stream.WithContext] ties the
// stream to a caller's context.
//
// Panics are handled differently: a panic in any producer or callback halts
// the dispatcher immediately, all pending callbacks are dropped, and Wait
// re-panics with a *[panics.Recovered]. A panic leaves no sensible slot to
// resume from, so halting is the only coherent response.
package stream
