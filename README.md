# `conker`: concurrency with the string attached

[![Go Reference](https://pkg.go.dev/badge/github.com/purpleclay/conker.svg)](https://pkg.go.dev/github.com/purpleclay/conker)
[![Go](https://img.shields.io/badge/Go_1.25+-00ADD8?logo=go&logoColor=white)](https://go.dev/)
[![MIT](https://img.shields.io/badge/MIT-gray?logo=github&logoColor=white)](LICENSE)

`conker` is a structured concurrency library for Go 1.25+, built on the lessons of [sourcegraph/conc](https://github.com/sourcegraph/conc) with a modernised stdlib-first approach.

```sh
go get github.com/purpleclay/conker
```

# At a glance

- Use [`pool.Pool`](https://pkg.go.dev/github.com/purpleclay/conker/pool#Pool) if you want bounded, panic-safe concurrent task execution with recursive submission and error aggregation.
- Use [`pool.ResultPool[T]`](https://pkg.go.dev/github.com/purpleclay/conker/pool#ResultPool) if tasks produce typed results and you want them back in submission order.
- Use [`stream.Stream`](https://pkg.go.dev/github.com/purpleclay/conker/stream#Stream) if you want concurrent producers with callbacks that execute strictly in submission order, each blocking until its slot's turn arrives.
- Use [`conker.WaitGroup`](https://pkg.go.dev/github.com/purpleclay/conker#WaitGroup) if you want a panic-safe replacement for `sync.WaitGroup`.
- Use [`panics.Catcher`](https://pkg.go.dev/github.com/purpleclay/conker/panics#Catcher) if you want to catch panics in goroutines you manage yourself.
- Use [`panics.ErrPanic`](https://pkg.go.dev/github.com/purpleclay/conker/panics#ErrPanic) with `errors.Is` to detect recovered panics without a type assertion.

# Goals

## Typed panics, not bare `any`

Go's `recover()` returns `any`. Every library that catches goroutine panics has to decide what to do with that opaque value, and the stdlib gives you nothing beyond the raw interface.

`conker` makes recovered panics first-class errors. `*panics.Recovered` implements `error` and integrates with `errors.Is` and `errors.As` — the same tools you already use for every other error in your program.

<table>
<tr>
<th><code>stdlib</code></th>
<th><code>conker</code></th>
</tr>
<tr>
<td>

```go
done := make(chan any, 1)
go func() {
    defer func() {
        done <- recover()
    }()
    doSomethingThatMightPanic()
}()

val := <-done
if val != nil {
    // val is any — type-switch required,
    // no errors.Is/As, stack lost
    panic(val)
}
```

</td>
<td>

```go
var c panics.Catcher
c.Try(doSomethingThatMightPanic)

if r := c.Recovered(); r != nil {
    // r is *panics.Recovered — a typed error
    errors.Is(r, panics.ErrPanic) // true
    errors.Is(r, ErrTimeout)      // true if panic value wrapped it
    log.Print(r.Stack)            // stack at panic site, not here
}
```

</td>
</tr>
</table>

## Panic-safe goroutines without boilerplate

Spawning goroutines safely with `sync.WaitGroup` requires a lot of ceremony: `Add`, `Done`, a deferred `recover`, and a decision about what to do with the panic value. `conker.WaitGroup` collapses all of that to one line.

Under the hood it delegates directly to `sync.WaitGroup.Go` — introduced in Go 1.25 — so there is no reimplementation of what the stdlib already does.

<table>
<tr>
<th><code>stdlib</code></th>
<th><code>conker</code></th>
</tr>
<tr>
<td>

```go
var wg sync.WaitGroup
for i := 0; i < 10; i++ {
    wg.Add(1)
    go func() {
        defer wg.Done()
        defer func() {
            if v := recover(); v != nil {
                // swallowed, or re-panic loses stack
            }
        }()
        doSomething()
    }()
}
wg.Wait()
```

</td>
<td>

```go
var wg conker.WaitGroup
for i := 0; i < 10; i++ {
    wg.Go(doSomething)
}
// re-panics as *panics.Recovered with
// the original stack trace preserved
wg.Wait()
```

</td>
</tr>
</table>

When you want to handle a panic rather than propagate it, use `WaitAndRecover`:

```go
var wg conker.WaitGroup
wg.Go(riskyOperation)

if r := wg.WaitAndRecover(); r != nil {
    log.Printf("goroutine panicked: %v\n%s", r.Value, r.Stack)
}
```

## Bounded concurrent tasks, not cancel-on-first-error

Running bounded concurrent tasks in Go typically means `errgroup` — and when used with `errgroup.WithContext`, it cancels remaining work on first error and still returns only one error from `Wait`.

`pool.Pool` collects every error from every task via `errors.Join`, recovers panics as typed `*panics.Recovered` errors, and passes each task its own context for per-task timeouts and backpressure via `GoCtx`.

<table>
<tr>
<th><code>errgroup</code></th>
<th><code>conker</code></th>
</tr>
<tr>
<td>

```go
g, ctx := errgroup.WithContext(context.Background())
g.SetLimit(10)
for _, item := range items {
    g.Go(func() error {
        return process(ctx, item)
    })
}
// cancels remaining tasks on first error;
// Wait returns only that one error;
// panics crash the process
if err := g.Wait(); err != nil { ... }
```

</td>
<td>

```go
p := pool.New().WithMaxGoroutines(10)
for _, item := range items {
    p.Go(func(ctx context.Context) error {
        return process(ctx, item)
    })
}
// collects all errors via errors.Join;
// panics surface as *panics.Recovered
if err := p.Wait(); err != nil { ... }
```

</td>
</tr>
</table>

## Concurrent processing with ordered callbacks

Processing items concurrently but emitting results in the original order typically requires buffering everything, sorting by index, and iterating only after all work is done.

`stream.Stream` separates the two concerns: producers run concurrently, but the dispatcher advances strictly in submission order — blocking on each slot until its producer finishes before calling the callback and moving to the next. A fast producer whose slot comes after a slow one waits; nothing fires out of sequence.

<table>
<tr>
<th><code>stdlib</code></th>
<th><code>conker</code></th>
</tr>
<tr>
<td>

```go
results := make([]string, len(items))
var wg sync.WaitGroup
for i, item := range items {
    wg.Add(1)
    go func(i int, item Item) {
        defer wg.Done()
        results[i] = process(item)
    }(i, item)
}
wg.Wait()
// ordered, but only after all work is done
for _, r := range results {
    emit(r)
}
```

</td>
<td>

```go
s := stream.New()
for _, item := range items {
    s.Go(func(ctx context.Context) stream.Callback {
        result := process(ctx, item) // concurrent
        return func() {
            emit(result) // ordered, fires when this slot's turn arrives
        }
    })
}
s.Wait()
```

</td>
</tr>
</table>

## All results collected, not silently dropped

`sourcegraph/conc`'s error-variant pools (`ResultErrorPool`, `ResultContextPool`) drop results from failed tasks by default. The `WithCollectErrored()` option exists specifically to opt back in — meaning silent data loss is the default behaviour.

`pool.ResultPool[T]` always returns every result alongside every error. Nothing is silently discarded because a task failed or panicked.

<table>
<tr>
<th><code>sourcegraph/conc</code></th>
<th><code>conker</code></th>
</tr>
<tr>
<td>

```go
p := pool.NewWithResults[string]().WithErrors()
for _, id := range ids {
    id := id
    p.Go(func() (string, error) {
        return fetch(id)
    })
}
results, err := p.Wait()
// results from failed tasks are silently
// dropped unless WithCollectErrored() is set
```

</td>
<td>

```go
p := pool.NewWithResults[string]()
for _, id := range ids {
    p.Go(func(_ context.Context) (string, error) {
        return fetch(id)
    })
}
results, err := p.Wait()
// all results returned alongside errors;
// nothing is silently dropped
```

</td>
</tr>
</table>

# Roadmap

`conker` is pre-1.0. The following milestones track progress toward a stable API:

| Milestone  | Theme             | Status      |
| ---------- | ----------------- | ----------- |
| **v0.1.0** | Foundations       | ✅          |
| **v0.2.0** | Pool              | ✅          |
| **v0.3.0** | Stream            | ✅          |
| **v0.4.0** | Iter              | In Progress |
| **v0.5.0** | Daemons & tooling | Planned     |

# Examples

Each example is a self-contained, runnable program. External clients are stubbed so there are no runtime dependencies beyond the Go standard library and `conker` itself.

| Example                                        | Feature              | Use case                                                                                                                                     |
| ---------------------------------------------- | -------------------- | -------------------------------------------------------------------------------------------------------------------------------------------- |
| [`etl-pipeline`](./examples/etl-pipeline/)     | `pool.ResultPool[T]` | ETL pipeline over an object store — download, transform, upload — with bounded concurrency, per-task timeouts, and submission-order results. |
| [`log-enrichment`](./examples/log-enrichment/) | `stream.Stream`      | Enrich a web server log stream concurrently — resolving each client IP to a country — while preserving the original chronological order.     |
