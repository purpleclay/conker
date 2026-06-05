// Package stream provides ordered concurrent processing: producer functions
// run concurrently, but their returned callbacks execute serially in
// submission order.
//
// Create a Stream with [New] and submit producers with [Stream.Go]. Call
// [Stream.Wait] to block until all producers have finished and all callbacks
// have run. Panics in either producers or callbacks propagate through Wait as
// *[panics.Recovered] values.
//
// Panic semantics differ from pool: a panic in any producer or callback halts
// the dispatcher immediately and all pending callbacks are dropped. Because
// callbacks must execute in submission order, skipping a failed slot and
// resuming the sequence is not possible — halting is the only coherent
// response.
package stream
