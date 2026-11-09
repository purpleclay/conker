package stream_test

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"pgregory.net/rapid"

	"github.com/purpleclay/conker/panics"
	"github.com/purpleclay/conker/stream"
)

// planRuns is how many times each generated plan runs. synctest pins time but
// not goroutine scheduling, so a scheduling bug may only fail some runs.
// Repeating the plan makes it fail consistently, which lets rapid shrink it.
const planRuns = 20

type outcome int

const (
	succeed outcome = iota
	nilCallback
	fail
	producerPanics
	callbackPanics
)

// callbackPanicBase offsets callback panic values from producer panic values,
// so the two can be told apart.
const callbackPanicBase = 1000

type step struct {
	outcome outcome
	goCtx   bool // submit with GoCtx and a live context rather than Go
	delay   time.Duration
}

type streamPlan struct {
	maxGoroutines int
	cancelOnError bool
	batches       [][]step // separated by Reset
}

// stepError is the error returned by a failing producer, identifying its step.
type stepError int

func (e stepError) Error() string { return "producer failed" }

func drawStreamPlan(t *rapid.T) streamPlan {
	// Lower draws shrink first, so the common cases sit at the bottom.
	outcomes := []outcome{succeed, succeed, succeed, nilCallback, fail, producerPanics, callbackPanics}
	return streamPlan{
		maxGoroutines: rapid.IntRange(1, 4).Draw(t, "maxGoroutines"),
		cancelOnError: rapid.Bool().Draw(t, "cancelOnError"),
		batches: rapid.SliceOfN(rapid.SliceOfN(rapid.Custom(func(t *rapid.T) step {
			return step{
				outcome: outcomes[rapid.IntRange(0, len(outcomes)-1).Draw(t, "outcome")],
				goCtx:   rapid.Bool().Draw(t, "goCtx"),
				// At least 1s, so which producers overlap depends on fake time
				// alone and not on goroutine scheduling.
				delay: time.Duration(rapid.IntRange(1, 4).Draw(t, "delay")) * time.Second,
			}
		}), 0, 16), 1, 3).Draw(t, "batches"),
	}
}

// batchObs is everything one batch did, recorded inside the bubble.
type batchObs struct {
	producerRuns []int
	startedAt    []time.Duration
	endedAt      []time.Duration // when each producer returned
	ctxErr       []error         // the cause each producer saw as it returned
	callbacks    []int           // steps whose callback ran, in the order they ran
	peak         int
	waitErr      error
	recovered    any // what Wait panicked with
}

func runStreamPlan(t *testing.T, plan streamPlan) []batchObs {
	t.Helper()
	var obs []batchObs
	synctest.Test(t, func(*testing.T) {
		start := time.Now()
		s := stream.New().WithMaxGoroutines(plan.maxGoroutines)
		if plan.cancelOnError {
			s.WithCancelOnError()
		}

		for b, steps := range plan.batches {
			if b > 0 {
				s.Reset()
			}
			n := len(steps)
			o := batchObs{
				producerRuns: make([]int, n), startedAt: make([]time.Duration, n),
				endedAt: make([]time.Duration, n), ctxErr: make([]error, n),
			}
			var mu sync.Mutex
			running := 0

			for i, st := range steps {
				producer := func(ctx context.Context) (stream.Callback, error) {
					mu.Lock()
					o.producerRuns[i]++
					o.startedAt[i] = time.Since(start)
					running++
					o.peak = max(o.peak, running)
					mu.Unlock()
					defer func() {
						mu.Lock()
						running--
						o.endedAt[i] = time.Since(start)
						o.ctxErr[i] = context.Cause(ctx)
						mu.Unlock()
					}()

					// Stop early on cancellation, so the end time shows when it arrived.
					select {
					case <-time.After(st.delay):
					case <-ctx.Done():
					}
					switch st.outcome {
					case nilCallback:
						return nil, nil
					case fail:
						return func() { panic("callback of a failed producer ran") }, stepError(i)
					case producerPanics:
						panic(i)
					}
					return func() {
						o.callbacks = append(o.callbacks, i) // callbacks run serially
						if st.outcome == callbackPanics {
							panic(callbackPanicBase + i)
						}
					}, nil
				}
				if st.goCtx {
					_ = s.GoCtx(context.Background(), producer)
				} else {
					s.Go(producer)
				}
			}

			func() {
				defer func() { o.recovered = recover() }()
				o.waitErr = s.Wait()
			}()
			obs = append(obs, o)
		}
	})
	return obs
}

func checkStreamBatch(t *rapid.T, plan streamPlan, steps []step, o batchObs) {
	n := len(steps)
	for i := range n {
		if o.producerRuns[i] != 1 {
			t.Fatalf("producer %d ran %d times, want 1", i, o.producerRuns[i])
		}
	}

	// Callbacks run in submission order up to the first panic, producer or
	// callback, and none run after it.
	halt := -1
	var wantCallbacks, wantErrs []int
	for i, st := range steps {
		if st.outcome == producerPanics {
			halt = i
			break
		}
		switch st.outcome {
		case succeed:
			wantCallbacks = append(wantCallbacks, i)
		case callbackPanics:
			wantCallbacks = append(wantCallbacks, i)
			halt = i
		case fail:
			wantErrs = append(wantErrs, i)
		}
		if halt >= 0 {
			break
		}
	}
	if !slices.Equal(o.callbacks, wantCallbacks) {
		t.Fatalf("callbacks ran for steps %v, want %v", o.callbacks, wantCallbacks)
	}

	if halt >= 0 {
		// A producer panic outranks a callback panic, even one discovered while
		// draining the slots after the halt.
		want := callbackPanicBase + halt
		for i := halt; i < n; i++ {
			if steps[i].outcome == producerPanics {
				want = i
				break
			}
		}
		r, ok := o.recovered.(*panics.Recovered)
		if !ok {
			t.Fatalf("Wait returned %v, want it to panic with %d", o.waitErr, want)
		}
		if r.Value != want {
			t.Fatalf("Wait panicked with %v, want %d", r.Value, want)
		}
	} else {
		if o.recovered != nil {
			t.Fatalf("Wait panicked with %v", o.recovered)
		}
		var gotErrs []int
		if o.waitErr != nil {
			joined, ok := o.waitErr.(interface{ Unwrap() []error })
			if !ok {
				t.Fatalf("Wait returned %v, want joined errors", o.waitErr)
			}
			for _, err := range joined.Unwrap() {
				gotErrs = append(gotErrs, int(err.(stepError)))
			}
		}
		if !slices.Equal(gotErrs, wantErrs) {
			t.Fatalf("Wait returned errors from steps %v, want %v", gotErrs, wantErrs)
		}
	}

	if o.peak > plan.maxGoroutines {
		t.Fatalf("peak producer concurrency %d exceeds limit %d", o.peak, plan.maxGoroutines)
	}
	if want := min(n, plan.maxGoroutines); o.peak < want {
		t.Fatalf("peak producer concurrency %d, want %d", o.peak, want)
	}

	// With WithCancelOnError, a producer that returns after the first failure
	// sees a cancelled context whose cause is a failure; one that returns before
	// it does not. Without it, no producer is ever cancelled. Producers stop
	// waiting on cancellation, so one still running at the first failure must
	// return at that instant, and one started after it must return at once.
	firstFailure := time.Duration(-1)
	failures := map[int]bool{}
	for i, st := range steps {
		if st.outcome == fail || st.outcome == producerPanics {
			failures[i] = true
			if firstFailure < 0 || o.endedAt[i] < firstFailure {
				firstFailure = o.endedAt[i]
			}
		}
	}
	for i, st := range steps {
		want := o.startedAt[i] + st.delay
		if plan.cancelOnError && firstFailure >= 0 {
			want = min(want, max(o.startedAt[i], firstFailure))
		}
		if o.endedAt[i] != want {
			t.Fatalf("producer %d started at %v and returned at %v, want %v (first failure at %v)",
				i, o.startedAt[i], o.endedAt[i], want, firstFailure)
		}

		cause := o.ctxErr[i]
		switch {
		case !plan.cancelOnError || firstFailure < 0 || o.endedAt[i] < firstFailure:
			if cause != nil {
				t.Fatalf("producer %d saw cause %v at %v, want a live context", i, cause, o.endedAt[i])
			}
		case o.endedAt[i] > firstFailure:
			if !failures[causeStep(cause)] {
				t.Fatalf("producer %d saw cause %v at %v, want a failure from before %v", i, cause, o.endedAt[i], firstFailure)
			}
		}
	}
}

// causeStep returns the step behind a cancellation cause, or -1.
func causeStep(cause error) int {
	var se stepError
	if errors.As(cause, &se) {
		return int(se)
	}
	var r *panics.Recovered
	if errors.As(cause, &r) {
		if v, ok := r.Value.(int); ok {
			return v
		}
	}
	return -1
}

// TestStream_Property runs random batches of producers, separated by Reset,
// that succeed, fail, panic, or return a nil or panicking callback. It checks
// that every producer runs once, callbacks run in submission order up to the
// first panic, Wait reports errors in submission order or re-panics with the
// right value, the producer limit holds and is reached, and WithCancelOnError
// cancels exactly the producers still running after the first failure.
func TestStream_Property(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		plan := drawStreamPlan(rt)
		for range planRuns {
			for b, o := range runStreamPlan(t, plan) {
				checkStreamBatch(rt, plan, plan.batches[b], o)
			}
		}
	})
}
