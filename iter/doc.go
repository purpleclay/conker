// Package iter provides concurrent iteration over Go 1.23 iterators
// ([iter.Seq] and [iter.Seq2]) and Go maps.
//
// Use [MapSeq] to transform a sequence concurrently while preserving order,
// [MapSeq2] for key-value pairs, [ForEachSeq] to run a side-effecting
// function over each element, [MapMap] to map over a Go map concurrently,
// and [ForEachMap] for side effects over a map.
//
// For error-returning and context-aware variants, use [MapSeqErr] and
// [ForEachSeqErr]. These pass a derived context into each fn call and collect
// errors via [errors.Join]. [WithCancelOnError] stops further dispatch and
// cancels the derived context for in-flight fn calls as soon as any fn call
// returns a non-nil error.
//
// All functions accept functional options to set the concurrency limit
// ([WithMaxGoroutines]) and a cancellation context ([WithContext]).
package iter
