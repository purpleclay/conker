// Package iter provides concurrent iteration over slices, Go 1.23 iterators
// ([iter.Seq] and [iter.Seq2]), and Go maps, with bounded concurrency and
// results in input order.
//
// Each operation comes in a form per input: [Map] and [ForEach] for slices,
// [MapSeq] and [ForEachSeq] for sequences, [MapSeq2] for key-value sequences,
// and [MapMap] and [ForEachMap] for maps. Maps have no defined order.
//
// Functions whose names end in Ctx pass a context into fn, derived from
// [WithContext], so in-flight work can observe cancellation. It is cancelled
// when the governing context is cancelled, when any fn call panics, and, for
// the lazy [MapSeqCtx] and [MapSeq2Ctx], when the caller leaves the range.
//
// Functions whose names end in Err also pass a context into fn, and collect
// errors via [errors.Join], each wrapped in an [ElemError] carrying the
// element's index. [WithCancelOnError] makes them fail fast.
//
// The remaining functions do not give fn a context; [WithContext] only stops
// new elements from being dispatched.
//
// Panics in fn are always recovered. Err functions return them as
// *[panics.Recovered] errors; the others stop dispatch and re-panic in the
// caller's goroutine once in-flight work has finished.
//
// A panic in the input sequence also reaches the caller. The Map functions
// range the input on a goroutine of their own, so they recover it, stop
// dispatch and re-panic it in the caller's goroutine as a
// *[panics.Recovered] once in-flight work has finished. [MapSeqErr] does the
// same rather than return an error, because the panic belongs to no element.
// The ForEach functions range the input in the caller's goroutine, so the
// panic propagates unchanged.
//
// The caller can leave the range of a lazy Map function by break, by a panic
// in the range body or by [runtime.Goexit] (t.Fatal calls it). Each one stops
// dispatch and waits for in-flight calls before the range exits. A panic in
// the body carries on unchanged, and a panic in fn during that wait is
// dropped.
package iter
