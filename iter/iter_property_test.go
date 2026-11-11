package iter_test

import (
	"context"
	"errors"
	"fmt"
	stditer "iter"
	"slices"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"pgregory.net/rapid"

	conkiter "github.com/purpleclay/conker/iter"
	"github.com/purpleclay/conker/panics"
)

// planRuns is how many times each generated plan runs. synctest pins time but
// not goroutine scheduling, so a scheduling bug may only fail some runs.
// Repeating the plan makes it fail consistently, which lets rapid shrink it.
const planRuns = 20

// elemFn is the shape every variant is driven through. Variants without a
// context or an error ignore the parts they cannot use.
type elemFn = func(context.Context, int) (int, error)

type resultOrder int

const (
	noResults resultOrder = iota
	inputOrder
	anyOrder
)

type variant struct {
	name    string
	results resultOrder
	errs    bool // an Err variant: failures and panics are returned as errors
	inMap   bool // iterates a map, so dispatch order is not input order
	// seqInput is set for variants that range a sequence the caller supplies
	// on a goroutine of their own, so the plan can make that sequence panic.
	seqInput bool
	run      func([]int, elemFn, []conkiter.Option) ([]int, error)
	seq      func([]int, elemFn, []conkiter.Option) stditer.Seq[int] // set for lazy variants
}

func plain(fn elemFn) func(int) int {
	return func(v int) int { r, _ := fn(context.Background(), v); return r }
}

func withCtx(fn elemFn) func(context.Context, int) int {
	return func(ctx context.Context, v int) int { r, _ := fn(ctx, v); return r }
}

func errOnly(fn elemFn) func(context.Context, int) error {
	return func(ctx context.Context, v int) error { _, err := fn(ctx, v); return err }
}

// pairValue is the value paired with key k in key-value inputs. It differs
// from every key, so a swapped or duplicated pair is caught by sameKey.
func pairValue(k int) int { return -k - 1 }

// sameKey checks that a key-value variant passed the pair through intact and
// returns the key, which is the element's index.
func sameKey(k, v int) int {
	if v != pairValue(k) {
		panic(fmt.Sprintf("key %d delivered with value %d", k, v))
	}
	return k
}

// inputPanics is an input element that makes the input sequence panic with
// inputPanicValue instead of yielding.
const inputPanics = -1

const inputPanicValue = "input sequence panics"

func values(in []int) stditer.Seq[int] {
	return func(yield func(int) bool) {
		for _, v := range in {
			if v == inputPanics {
				panic(inputPanicValue)
			}
			if !yield(v) {
				return
			}
		}
	}
}

func pairs(in []int) stditer.Seq2[int, int] {
	return func(yield func(int, int) bool) {
		for _, k := range in {
			if k == inputPanics {
				panic(inputPanicValue)
			}
			if !yield(k, pairValue(k)) {
				return
			}
		}
	}
}

func asMap(in []int) map[int]int {
	m := make(map[int]int, len(in))
	for _, k := range in {
		m[k] = pairValue(k)
	}
	return m
}

func lazy(name string, seq func([]int, elemFn, []conkiter.Option) stditer.Seq[int]) variant {
	return variant{
		name:     name,
		results:  inputOrder,
		seqInput: true,
		seq:      seq,
		run: func(in []int, fn elemFn, o []conkiter.Option) ([]int, error) {
			return slices.Collect(seq(in, fn, o)), nil
		},
	}
}

var variants = []variant{
	{name: "Map", results: inputOrder, run: func(in []int, fn elemFn, o []conkiter.Option) ([]int, error) {
		return conkiter.Map(in, plain(fn), o...), nil
	}},
	{name: "MapCtx", results: inputOrder, run: func(in []int, fn elemFn, o []conkiter.Option) ([]int, error) {
		return conkiter.MapCtx(in, withCtx(fn), o...), nil
	}},
	{name: "MapErr", results: inputOrder, errs: true, run: func(in []int, fn elemFn, o []conkiter.Option) ([]int, error) {
		return conkiter.MapErr(in, fn, o...)
	}},
	{name: "MapSeqErr", results: inputOrder, errs: true, seqInput: true, run: func(in []int, fn elemFn, o []conkiter.Option) ([]int, error) {
		return conkiter.MapSeqErr(values(in), fn, o...)
	}},
	lazy("MapSeq", func(in []int, fn elemFn, o []conkiter.Option) stditer.Seq[int] {
		return conkiter.MapSeq(values(in), plain(fn), o...)
	}),
	lazy("MapSeqCtx", func(in []int, fn elemFn, o []conkiter.Option) stditer.Seq[int] {
		return conkiter.MapSeqCtx(values(in), withCtx(fn), o...)
	}),
	lazy("MapSeq2", func(in []int, fn elemFn, o []conkiter.Option) stditer.Seq[int] {
		return conkiter.MapSeq2(pairs(in), func(k, v int) int { return plain(fn)(sameKey(k, v)) }, o...)
	}),
	lazy("MapSeq2Ctx", func(in []int, fn elemFn, o []conkiter.Option) stditer.Seq[int] {
		return conkiter.MapSeq2Ctx(pairs(in), func(ctx context.Context, k, v int) int { return withCtx(fn)(ctx, sameKey(k, v)) }, o...)
	}),
	{name: "MapMap", results: anyOrder, inMap: true, run: func(in []int, fn elemFn, o []conkiter.Option) ([]int, error) {
		return conkiter.MapMap(asMap(in), func(k, v int) int { return plain(fn)(sameKey(k, v)) }, o...), nil
	}},
	{name: "MapMapCtx", results: anyOrder, inMap: true, run: func(in []int, fn elemFn, o []conkiter.Option) ([]int, error) {
		return conkiter.MapMapCtx(asMap(in), func(ctx context.Context, k, v int) int { return withCtx(fn)(ctx, sameKey(k, v)) }, o...), nil
	}},
	{name: "ForEach", run: func(in []int, fn elemFn, o []conkiter.Option) ([]int, error) {
		conkiter.ForEach(in, func(v int) { plain(fn)(v) }, o...)
		return nil, nil
	}},
	{name: "ForEachCtx", run: func(in []int, fn elemFn, o []conkiter.Option) ([]int, error) {
		conkiter.ForEachCtx(in, func(ctx context.Context, v int) { withCtx(fn)(ctx, v) }, o...)
		return nil, nil
	}},
	{name: "ForEachErr", errs: true, run: func(in []int, fn elemFn, o []conkiter.Option) ([]int, error) {
		return nil, conkiter.ForEachErr(in, errOnly(fn), o...)
	}},
	{name: "ForEachSeq", run: func(in []int, fn elemFn, o []conkiter.Option) ([]int, error) {
		conkiter.ForEachSeq(slices.Values(in), func(v int) { plain(fn)(v) }, o...)
		return nil, nil
	}},
	{name: "ForEachSeqCtx", run: func(in []int, fn elemFn, o []conkiter.Option) ([]int, error) {
		conkiter.ForEachSeqCtx(slices.Values(in), func(ctx context.Context, v int) { withCtx(fn)(ctx, v) }, o...)
		return nil, nil
	}},
	{name: "ForEachSeqErr", errs: true, run: func(in []int, fn elemFn, o []conkiter.Option) ([]int, error) {
		return nil, conkiter.ForEachSeqErr(slices.Values(in), errOnly(fn), o...)
	}},
	{name: "ForEachMap", inMap: true, run: func(in []int, fn elemFn, o []conkiter.Option) ([]int, error) {
		conkiter.ForEachMap(asMap(in), func(k, v int) { plain(fn)(sameKey(k, v)) }, o...)
		return nil, nil
	}},
	{name: "ForEachMapCtx", inMap: true, run: func(in []int, fn elemFn, o []conkiter.Option) ([]int, error) {
		conkiter.ForEachMapCtx(asMap(in), func(ctx context.Context, k, v int) { withCtx(fn)(ctx, sameKey(k, v)) }, o...)
		return nil, nil
	}},
}

type outcome int

const (
	succeed outcome = iota
	fail
	panicked
)

type elem struct {
	outcome outcome
	delay   time.Duration
}

type iterPlan struct {
	variant       variant
	maxGoroutines int
	cancelOnError bool
	preCancel     bool // cancel the parent context before the call
	cancelAt      int  // element whose fn cancels the parent context as it starts; -1 for none
	breakAfter    int  // lazy variants: stop ranging after this many results; 0 for never
	inputPanicAt  int  // seqInput variants: the input panics after yielding this many elements; -1 for never
	elems         []elem
}

// elemError is the error a failing element returns, identifying it.
type elemError int

func (e elemError) Error() string { return "element failed" }

func drawIterPlan(t *rapid.T) iterPlan {
	p := iterPlan{
		variant:       rapid.SampledFrom(variants).Draw(t, "variant"),
		maxGoroutines: rapid.IntRange(1, 4).Draw(t, "maxGoroutines"),
		cancelAt:      -1,
		inputPanicAt:  -1,
	}
	outcomes := []outcome{succeed, succeed, succeed, panicked}
	if p.variant.errs {
		outcomes = []outcome{succeed, succeed, succeed, fail, panicked}
		p.cancelOnError = rapid.Bool().Draw(t, "cancelOnError")
	}
	p.elems = rapid.SliceOfN(rapid.Custom(func(t *rapid.T) elem {
		return elem{
			outcome: outcomes[rapid.IntRange(0, len(outcomes)-1).Draw(t, "outcome")],
			// At least 1s, so which elements overlap depends on fake time alone
			// and not on goroutine scheduling.
			delay: time.Duration(rapid.IntRange(1, 4).Draw(t, "delay")) * time.Second,
		}
	}), 0, 16).Draw(t, "elems")

	p.preCancel = rapid.IntRange(0, 9).Draw(t, "preCancel") == 0
	if n := len(p.elems); n > 0 && !p.preCancel {
		if rapid.Bool().Draw(t, "cancelParent") {
			p.cancelAt = rapid.IntRange(0, n-1).Draw(t, "cancelAt")
		}
		if p.variant.seq != nil {
			p.breakAfter = rapid.IntRange(0, n).Draw(t, "breakAfter")
		}
	}
	if p.variant.seqInput && !p.preCancel && rapid.Bool().Draw(t, "inputPanics") {
		p.inputPanicAt = rapid.IntRange(0, len(p.elems)).Draw(t, "inputPanicAt")
	}
	return p
}

// iterObs is everything one run did, recorded inside the bubble.
type iterObs struct {
	starts     [][]time.Duration // start times of each fn call, per element
	ends       []time.Duration   // when each element's last fn call returned
	returnedAt time.Duration     // when the iter function returned
	peak       int
	results    []int
	err        error
	recovered  any           // what the call itself panicked with
	brokeAt    time.Duration // when the range was broken; -1 if it was not
}

func runIterPlan(t *testing.T, plan iterPlan) iterObs {
	t.Helper()
	n := len(plan.elems)
	o := iterObs{starts: make([][]time.Duration, n), ends: make([]time.Duration, n), brokeAt: -1}
	synctest.Test(t, func(*testing.T) {
		start := time.Now()
		parent, cancel := context.WithCancel(context.Background())
		defer cancel()

		var mu sync.Mutex
		running := 0
		fn := func(_ context.Context, i int) (int, error) {
			mu.Lock()
			o.starts[i] = append(o.starts[i], time.Since(start))
			running++
			o.peak = max(o.peak, running)
			mu.Unlock()
			defer func() {
				mu.Lock()
				running--
				o.ends[i] = time.Since(start)
				mu.Unlock()
			}()

			if i == plan.cancelAt {
				cancel()
			}
			time.Sleep(plan.elems[i].delay)
			switch plan.elems[i].outcome {
			case fail:
				return i + 1, elemError(i) // the value must still be dropped
			case panicked:
				panic(i)
			}
			return i + 1, nil
		}

		options := []conkiter.Option{conkiter.WithMaxGoroutines(plan.maxGoroutines), conkiter.WithContext(parent)}
		if plan.cancelOnError {
			options = append(options, conkiter.WithCancelOnError())
		}
		in := make([]int, n)
		for i := range in {
			in[i] = i
		}
		if plan.inputPanicAt >= 0 {
			in = append(in[:plan.inputPanicAt], inputPanics)
		}
		if plan.preCancel {
			cancel()
		}

		func() {
			defer func() {
				o.recovered = recover()
				o.returnedAt = time.Since(start)
			}()
			if plan.breakAfter == 0 {
				o.results, o.err = plan.variant.run(in, fn, options)
				return
			}
			for r := range plan.variant.seq(in, fn, options) {
				o.results = append(o.results, r)
				if len(o.results) == plan.breakAfter {
					o.brokeAt = time.Since(start)
					break
				}
			}
		}()
	})
	return o
}

func checkIterRun(t *rapid.T, plan iterPlan, o iterObs) {
	n := len(plan.elems)

	// Each element runs at most once, and every call has returned by the time
	// the function does.
	var dispatched []int
	for i := range n {
		switch {
		case len(o.starts[i]) > 1:
			t.Fatalf("element %d ran %d times", i, len(o.starts[i]))
		case len(o.starts[i]) == 1 && o.ends[i] > o.returnedAt:
			t.Fatalf("element %d was still running when %s returned at %v", i, plan.variant.name, o.returnedAt)
		case len(o.starts[i]) == 1:
			dispatched = append(dispatched, i)
		}
	}
	for k, i := range dispatched {
		if !plan.variant.inMap && i != k {
			t.Fatalf("dispatched elements %v are not a prefix of the input", dispatched)
		}
	}

	// The call may re-panic with the input's panic only once the input has
	// yielded every element before it, and each of those has been dispatched.
	r, _ := o.recovered.(*panics.Recovered)
	inputRepanicked := r != nil && r.Value == inputPanicValue
	if inputRepanicked {
		if plan.inputPanicAt < 0 {
			t.Fatalf("re-panicked with the input's panic, but the input does not panic")
		}
		if len(dispatched) != plan.inputPanicAt {
			t.Fatalf("the input panicked after %d elements, but %d were dispatched", plan.inputPanicAt, len(dispatched))
		}
	}
	yielded := n // elements the input yields before it panics or ends
	if plan.inputPanicAt >= 0 {
		yielded = plan.inputPanicAt
	}

	if plan.preCancel {
		if len(dispatched) > 0 || len(o.results) > 0 || o.err != nil || o.recovered != nil {
			t.Fatalf("with a cancelled context: dispatched %v, results %v, error %v, panic %v; want nothing", dispatched, o.results, o.err, o.recovered)
		}
		return
	}

	// Dispatch stops at the first stop signal: nothing may start after it.
	stopAt := time.Duration(-1)
	stop := func(at time.Duration) {
		if stopAt < 0 || at < stopAt {
			stopAt = at
		}
	}
	var panickedElems, failedElems []int
	for _, i := range dispatched {
		e, startedAt := plan.elems[i], o.starts[i][0]
		switch {
		case e.outcome == panicked && !plan.variant.errs:
			stop(startedAt + e.delay)
		case e.outcome != succeed && plan.cancelOnError:
			stop(startedAt + e.delay)
		}
		if i == plan.cancelAt {
			stop(startedAt)
		}
		switch e.outcome {
		case panicked:
			panickedElems = append(panickedElems, i)
		case fail:
			failedElems = append(failedElems, i)
		}
	}
	if o.brokeAt >= 0 {
		stop(o.brokeAt)
	}
	if stopAt < 0 && len(dispatched) != yielded {
		t.Fatalf("dispatched %d of %d elements with nothing to stop dispatch", len(dispatched), yielded)
	}
	if stopAt < 0 && plan.inputPanicAt >= 0 && !inputRepanicked {
		t.Fatalf("the input panicked with nothing to stop dispatch first, but %s did not re-panic it", plan.variant.name)
	}
	for _, i := range dispatched {
		if stopAt >= 0 && o.starts[i][0] > stopAt {
			t.Fatalf("element %d started at %v, after dispatch stopped at %v", i, o.starts[i][0], stopAt)
		}
	}

	if o.peak > plan.maxGoroutines {
		t.Fatalf("peak concurrency %d exceeds limit %d", o.peak, plan.maxGoroutines)
	}
	if want := min(yielded, plan.maxGoroutines); plan.cancelAt < 0 && o.peak < want {
		t.Fatalf("peak concurrency %d, want %d", o.peak, want)
	}

	if inputRepanicked {
		return // no results or errors to check
	}

	if !plan.variant.errs {
		if len(panickedElems) == 0 {
			if o.recovered != nil {
				t.Fatalf("unexpected panic %v", o.recovered)
			}
		} else {
			r, ok := o.recovered.(*panics.Recovered)
			if !ok {
				t.Fatalf("%s returned normally, want the panic from one of elements %v", plan.variant.name, panickedElems)
			}
			if !slices.Contains(panickedElems, r.Value.(int)) {
				t.Fatalf("re-panicked with %v, want one of elements %v", r.Value, panickedElems)
			}
			return // no results to check
		}
	} else if o.recovered != nil {
		t.Fatalf("unexpected panic %v", o.recovered)
	}

	var want []int
	for _, i := range dispatched {
		if plan.elems[i].outcome == succeed {
			want = append(want, i+1)
		} else {
			want = append(want, 0) // a failed element leaves a zero-value hole
		}
	}
	if o.brokeAt >= 0 {
		want = want[:min(plan.breakAfter, len(want))]
	}
	switch plan.variant.results {
	case inputOrder:
		if !slices.Equal(o.results, want) {
			t.Fatalf("results %v, want %v", o.results, want)
		}
	case anyOrder:
		if !slices.Equal(slices.Sorted(slices.Values(o.results)), slices.Sorted(slices.Values(want))) {
			t.Fatalf("results %v, want %v in any order", o.results, want)
		}
	}

	if !plan.variant.errs {
		return
	}
	wantIDs := slices.Sorted(slices.Values(append(failedElems, panickedElems...)))
	var gotIDs []int
	if o.err != nil {
		joined, ok := o.err.(interface{ Unwrap() []error })
		if !ok {
			t.Fatalf("error %v is not joined", o.err)
		}
		for _, err := range joined.Unwrap() {
			var ee *conkiter.ElemError
			if !errors.As(err, &ee) {
				t.Fatalf("error %v is not an ElemError", err)
			}
			var id int
			var r *panics.Recovered
			if errors.As(ee.Err, &r) {
				id = r.Value.(int)
			} else {
				id = int(ee.Err.(elemError))
			}
			if id != ee.Index {
				t.Fatalf("ElemError index %d wraps the error from element %d", ee.Index, id)
			}
			gotIDs = append(gotIDs, ee.Index)
		}
	}
	if !slices.Equal(gotIDs, wantIDs) {
		t.Fatalf("ElemError indexes %v, want %v", gotIDs, wantIDs)
	}
}

// TestIter_Property drives every iter function through random inputs,
// concurrency limits, failures, panics, parent cancellation, and early breaks.
// It checks that elements are dispatched in input order and never after a stop
// signal, that results and ElemErrors line up with the elements that ran, that
// panics in fn or in the input re-surface, and that the concurrency limit
// holds and is reached.
func TestIter_Property(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		plan := drawIterPlan(rt)
		for range planRuns {
			checkIterRun(rt, plan, runIterPlan(t, plan))
		}
	})
}
