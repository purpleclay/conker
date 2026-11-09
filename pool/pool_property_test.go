package pool_test

import (
	"cmp"
	"context"
	"errors"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"pgregory.net/rapid"

	"github.com/purpleclay/conker/panics"
	"github.com/purpleclay/conker/pool"
)

// planRuns is how many times each generated plan runs. synctest pins time but
// not goroutine scheduling, so a scheduling bug may only fail some runs.
// Repeating the plan makes it fail consistently, which lets rapid shrink it.
const planRuns = 20

type submitKind int

const (
	submitGo submitKind = iota
	submitGoCtx
	submitGoCtxCancelled
	submitTryGo
	cancelParent
)

type outcome int

const (
	succeed outcome = iota
	fail
	panicked
	untilCancelled // succeeds after its delay unless cancelled first
)

type step struct {
	kind    submitKind
	outcome outcome
	delay   time.Duration
}

type poolPlan struct {
	results       bool // ResultPool[int] rather than Pool
	unordered     bool
	capacity      int // WithCapacity, 0 for unset
	maxGoroutines int
	cancelOnError bool
	firstError    bool
	batches       [][]step // separated by Reset
}

// stepError is the error returned by a failing task, identifying its step.
type stepError int

func (e stepError) Error() string { return "step failed" }

// cancelledError is returned by an untilCancelled task whose context was
// cancelled, wrapping ctx.Err() and identifying its step.
type cancelledError struct {
	step int
	err  error
}

func (e cancelledError) Error() string { return "step cancelled: " + e.err.Error() }
func (e cancelledError) Unwrap() error { return e.err }

func drawStep(t *rapid.T) step {
	// Lower draws shrink first, so the common cases sit at the bottom.
	kinds := []submitKind{submitGo, submitGo, submitGoCtx, submitTryGo, submitTryGo, submitGoCtxCancelled, cancelParent}
	outcomes := []outcome{succeed, succeed, succeed, fail, panicked, untilCancelled}
	return step{
		kind:    kinds[rapid.IntRange(0, len(kinds)-1).Draw(t, "kind")],
		outcome: outcomes[rapid.IntRange(0, len(outcomes)-1).Draw(t, "outcome")],
		// At least 1s, so which tasks overlap depends on fake time alone and
		// not on goroutine scheduling.
		delay: time.Duration(rapid.IntRange(1, 4).Draw(t, "delay")) * time.Second,
	}
}

func drawPoolPlan(t *rapid.T) poolPlan {
	p := poolPlan{
		results:       rapid.Bool().Draw(t, "results"),
		maxGoroutines: rapid.IntRange(1, 4).Draw(t, "maxGoroutines"),
		cancelOnError: rapid.Bool().Draw(t, "cancelOnError"),
		firstError:    rapid.Bool().Draw(t, "firstError"),
	}
	if p.results {
		p.unordered = rapid.Bool().Draw(t, "unordered")
		p.capacity = rapid.IntRange(0, 8).Draw(t, "capacity")
	}
	p.batches = rapid.SliceOfN(rapid.SliceOfN(rapid.Custom(drawStep), 0, 20), 1, 3).Draw(t, "batches")
	return p
}

// poolSubject drives Pool and ResultPool[int] through one interface.
type poolSubject struct {
	goFn   func(func(context.Context) (int, error))
	goCtx  func(context.Context, func(context.Context) (int, error)) error
	tryGo  func(func(context.Context) (int, error)) bool
	wait   func() ([]int, error)
	errors func() []error
	reset  func()
}

func newPoolSubject(parent context.Context, plan poolPlan) poolSubject {
	if plan.results {
		p := pool.NewWithResults[int]().WithMaxGoroutines(plan.maxGoroutines).WithContext(parent)
		if plan.unordered {
			p.WithUnorderedResults()
		}
		if plan.capacity > 0 {
			p.WithCapacity(plan.capacity)
		}
		if plan.cancelOnError {
			p.WithCancelOnError()
		}
		if plan.firstError {
			p.WithFirstError()
		}
		return poolSubject{p.Go, p.GoCtx, p.TryGo, p.Wait, p.Errors, p.Reset}
	}

	p := pool.New().WithMaxGoroutines(plan.maxGoroutines).WithContext(parent)
	if plan.cancelOnError {
		p.WithCancelOnError()
	}
	if plan.firstError {
		p.WithFirstError()
	}
	wrap := func(fn func(context.Context) (int, error)) func(context.Context) error {
		return func(ctx context.Context) error {
			_, err := fn(ctx)
			return err
		}
	}
	return poolSubject{
		goFn:   func(fn func(context.Context) (int, error)) { p.Go(wrap(fn)) },
		goCtx:  func(ctx context.Context, fn func(context.Context) (int, error)) error { return p.GoCtx(ctx, wrap(fn)) },
		tryGo:  func(fn func(context.Context) (int, error)) bool { return p.TryGo(wrap(fn)) },
		wait:   func() ([]int, error) { return nil, p.Wait() },
		errors: p.Errors,
		reset:  p.Reset,
	}
}

// batchObs is everything one batch did, recorded inside the bubble.
type batchObs struct {
	accepted        []bool
	runs            []int32
	endedAt         []time.Duration // when each task returned
	cancelled       []bool          // untilCancelled tasks that saw cancellation
	peak            int
	parentCancelled bool // before or during this batch
	taskCtx         context.Context
	results         []int
	waitErr         error
	errs            []error
}

func runPoolPlan(t *testing.T, plan poolPlan) []batchObs {
	t.Helper()
	var obs []batchObs
	synctest.Test(t, func(*testing.T) {
		start := time.Now()
		parent, cancel := context.WithCancel(context.Background())
		defer cancel()
		s := newPoolSubject(parent, plan)

		parentCancelled := false
		for b, steps := range plan.batches {
			if b > 0 {
				s.reset()
			}
			o := batchObs{accepted: make([]bool, len(steps)), runs: make([]int32, len(steps)), endedAt: make([]time.Duration, len(steps)), cancelled: make([]bool, len(steps))}
			runs := make([]atomic.Int32, len(steps))
			var mu sync.Mutex
			running := 0

			for i, st := range steps {
				task := func(ctx context.Context) (int, error) {
					runs[i].Add(1)
					mu.Lock()
					running++
					o.peak = max(o.peak, running)
					if o.taskCtx == nil {
						o.taskCtx = ctx
					}
					mu.Unlock()
					defer func() {
						mu.Lock()
						running--
						o.endedAt[i] = time.Since(start)
						mu.Unlock()
					}()

					if st.outcome == untilCancelled {
						select {
						case <-time.After(st.delay):
							return i + 1, nil
						case <-ctx.Done():
							mu.Lock()
							o.cancelled[i] = true
							mu.Unlock()
							return i + 1, cancelledError{i, ctx.Err()}
						}
					}
					time.Sleep(st.delay)
					switch st.outcome {
					case fail:
						return i + 1, stepError(i)
					case panicked:
						panic(i)
					}
					return i + 1, nil
				}

				switch st.kind {
				case submitGo:
					s.goFn(task)
					o.accepted[i] = true
				case submitGoCtx:
					o.accepted[i] = s.goCtx(context.Background(), task) == nil
				case submitGoCtxCancelled:
					ctx, c := context.WithCancel(context.Background())
					c()
					o.accepted[i] = s.goCtx(ctx, task) == nil
				case submitTryGo:
					o.accepted[i] = s.tryGo(task)
				case cancelParent:
					cancel()
					parentCancelled = true
				}
			}

			o.results, o.waitErr = s.wait()
			o.errs = s.errors()
			o.parentCancelled = parentCancelled
			for i := range runs {
				o.runs[i] = runs[i].Load()
			}
			obs = append(obs, o)
		}
	})
	return obs
}

// stepID identifies which step produced err: a stepError or a recovered panic.
func stepID(t *rapid.T, err error) int {
	var se stepError
	if errors.As(err, &se) {
		return int(se)
	}
	var ce cancelledError
	if errors.As(err, &ce) {
		return ce.step
	}
	var r *panics.Recovered
	if errors.As(err, &r) {
		return r.Value.(int)
	}
	t.Fatalf("unexpected error %v", err)
	return -1
}

func checkBatch(t *rapid.T, plan poolPlan, steps []step, o batchObs) {
	var wantResults, wantErrIDs []int
	lead := 0 // accepted submissions before the first rejection
	leading := true
	// Every task sleeps for at least 1s, so fake time stands still until a
	// blocking submission waits for a slot. Until then, every accepted task is
	// still running and the number of free slots is known exactly.
	inUse, clockMoved := 0, false
	for i, st := range steps {
		if st.kind == cancelParent {
			continue
		}
		if !clockMoved {
			free := inUse < plan.maxGoroutines
			switch st.kind {
			case submitTryGo:
				if o.accepted[i] != free {
					t.Fatalf("step %d: TryGo with %d of %d slots in use returned %v", i, inUse, plan.maxGoroutines, o.accepted[i])
				}
			case submitGoCtxCancelled:
				if !free && o.accepted[i] {
					t.Fatalf("step %d: GoCtx with a cancelled context was accepted by a full pool", i)
				}
			default:
				clockMoved = !free
			}
			if o.accepted[i] {
				inUse++
			}
		}
		if !o.accepted[i] {
			if st.kind == submitGo || st.kind == submitGoCtx {
				t.Fatalf("step %d: blocking submission was rejected", i)
			}
			if o.runs[i] != 0 {
				t.Fatalf("step %d: rejected submission ran %d times", i, o.runs[i])
			}
			leading = false
			continue
		}
		if o.runs[i] != 1 {
			t.Fatalf("step %d: accepted submission ran %d times, want 1", i, o.runs[i])
		}
		if o.cancelled[i] {
			// It ended without the clock moving, so from here free slots and
			// overlap are no longer known exactly.
			clockMoved, leading = true, false
		}
		if leading {
			lead++
		}
		switch st.outcome {
		case succeed:
			wantResults = append(wantResults, i+1)
		case untilCancelled:
			wantResults = append(wantResults, i+1)
			if o.cancelled[i] {
				wantErrIDs = append(wantErrIDs, i)
			}
		case fail:
			wantResults = append(wantResults, i+1)
			wantErrIDs = append(wantErrIDs, i)
		case panicked:
			wantResults = append(wantResults, 0) // a panicking task records the zero value
			wantErrIDs = append(wantErrIDs, i)
		}
	}

	if o.peak > plan.maxGoroutines {
		t.Fatalf("peak concurrency %d exceeds limit %d", o.peak, plan.maxGoroutines)
	}
	if want := min(lead, plan.maxGoroutines); o.peak < want {
		t.Fatalf("peak concurrency %d, want at least %d", o.peak, want)
	}

	errIDs := make([]int, len(o.errs))
	for i, err := range o.errs {
		errIDs[i] = stepID(t, err)
	}
	ordered := plan.results && !plan.unordered

	if plan.results {
		got := o.results
		if !ordered {
			got = slices.Sorted(slices.Values(got))
			wantResults = slices.Sorted(slices.Values(wantResults))
		}
		if !slices.Equal(got, wantResults) {
			t.Fatalf("results %v, want %v", o.results, wantResults)
		}
	}

	gotIDs := errIDs
	if !ordered {
		gotIDs = slices.Sorted(slices.Values(errIDs))
	}
	if !slices.Equal(gotIDs, wantErrIDs) {
		t.Fatalf("Errors() from steps %v, want %v", errIDs, wantErrIDs)
	}

	switch {
	case len(o.errs) == 0:
		if o.waitErr != nil {
			t.Fatalf("Wait returned %v with no task errors", o.waitErr)
		}
	case plan.firstError:
		if !slices.Contains(o.errs, o.waitErr) {
			t.Fatalf("WithFirstError: Wait returned %v, not one of Errors()", o.waitErr)
		}
		// The failure that cancelled the siblings must win over the errors they
		// return on seeing that cancellation.
		var ce cancelledError
		if plan.cancelOnError && !o.parentCancelled && errors.As(o.waitErr, &ce) {
			t.Fatalf("WithFirstError: Wait returned %v from a sibling cancelled by a failure", o.waitErr)
		}
		if !ordered && o.waitErr != o.errs[0] {
			t.Fatalf("WithFirstError: Wait returned step %d, want first recorded step %d", stepID(t, o.waitErr), errIDs[0])
		}
		// Errors that finish at the same instant may be recorded in either order.
		first := slices.MinFunc(errIDs, func(a, b int) int { return cmp.Compare(o.endedAt[a], o.endedAt[b]) })
		if got := stepID(t, o.waitErr); o.endedAt[got] != o.endedAt[first] {
			t.Fatalf("WithFirstError: Wait returned step %d, which finished at %v, but step %d finished first at %v",
				got, o.endedAt[got], first, o.endedAt[first])
		}
	default:
		joined, ok := o.waitErr.(interface{ Unwrap() []error })
		if !ok || !slices.Equal(joined.Unwrap(), o.errs) {
			t.Fatalf("Wait returned %v, want errors.Join(Errors()...)", o.waitErr)
		}
	}

	if o.taskCtx == nil {
		return
	}
	cause := context.Cause(o.taskCtx)
	switch {
	case o.parentCancelled:
		if cause != context.Canceled && !(plan.cancelOnError && slices.Contains(o.errs, cause)) {
			t.Fatalf("cause %v after the parent was cancelled", cause)
		}
	case plan.cancelOnError && len(o.errs) > 0:
		if !slices.Contains(o.errs, cause) {
			t.Fatalf("WithCancelOnError: cause %v is not a task error", cause)
		}
		if plan.firstError && cause != o.waitErr {
			t.Fatalf("WithCancelOnError and WithFirstError: cause %v, Wait returned %v", cause, o.waitErr)
		}
	default:
		if cause != pool.ErrPoolClosed {
			t.Fatalf("cause %v, want ErrPoolClosed", cause)
		}
	}
}

// TestPool_Property_Batches runs random sequences of submissions, parent
// cancellation, and Reset against Pool and ResultPool, checking that every
// accepted task runs exactly once, results and errors line up with what was
// submitted, the concurrency limit holds and is reached, and Wait and the
// cancellation cause follow the configured error handling.
func TestPool_Property_Batches(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		plan := drawPoolPlan(rt)
		for range planRuns {
			for b, o := range runPoolPlan(t, plan) {
				checkBatch(rt, plan, plan.batches[b], o)
			}
		}
	})
}

// TestPool_Property_RecursiveSubmission builds a random task tree where each
// task submits its children, and checks that Wait covers the whole tree and
// every task runs exactly once. Children are submitted either with TryGo,
// running inline when the pool is full, at any concurrency limit; or with Go,
// with a slot for every task so the recursion cannot deadlock.
func TestPool_Property_RecursiveSubmission(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		n := rapid.IntRange(1, 30).Draw(rt, "nodes")
		blocking := rapid.Bool().Draw(rt, "blocking")
		maxGoroutines := n
		if !blocking {
			maxGoroutines = rapid.IntRange(1, 3).Draw(rt, "maxGoroutines")
		}
		children := make([][]int, n)
		delays := make([]time.Duration, n)
		for i := range n {
			if i > 0 {
				parent := rapid.IntRange(0, i-1).Draw(rt, "parent")
				children[parent] = append(children[parent], i)
			}
			delays[i] = time.Duration(rapid.IntRange(0, 2).Draw(rt, "delay")) * time.Second
		}

		for range planRuns {
			runs := make([]atomic.Int32, n)
			var waitErr error
			synctest.Test(t, func(*testing.T) {
				p := pool.New().WithMaxGoroutines(maxGoroutines)
				var node func(i int) func(context.Context) error
				node = func(i int) func(context.Context) error {
					return func(ctx context.Context) error {
						runs[i].Add(1)
						time.Sleep(delays[i])
						for _, c := range children[i] {
							if blocking {
								p.Go(node(c))
							} else if !p.TryGo(node(c)) {
								_ = node(c)(ctx)
							}
						}
						return nil
					}
				}
				p.Go(node(0))
				waitErr = p.Wait()
			})

			if waitErr != nil {
				rt.Fatalf("Wait returned %v", waitErr)
			}
			for i := range runs {
				if got := runs[i].Load(); got != 1 {
					rt.Fatalf("node %d ran %d times, want 1", i, got)
				}
			}
		}
	})
}
