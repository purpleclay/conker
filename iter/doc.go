// Package iter provides concurrent iteration over Go 1.23 iterators
// ([iter.Seq] and [iter.Seq2]) and Go maps.
//
// Use [MapSeq] to transform a sequence concurrently while preserving order,
// [MapSeq2] for key-value pairs, [ForEachSeq] to run a side-effecting
// function over each element, [MapMap] to map over a Go map concurrently,
// and [ForEachMap] for side effects over a map. All functions accept
// functional options to set the concurrency limit ([WithMaxGoroutines]) and
// a cancellation context ([WithContext]).
package iter
