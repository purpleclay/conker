# ETL Pipeline using `pool.ResultPool[T]`

An ETL pipeline that processes a continuous stream of objects from a store — download, transform, upload — with bounded concurrency, per-task timeouts, and guaranteed submission-order results.

## The use case

You have a store with a continuous or very large stream of objects and a per-object transformation: a thumbnail to resize, a JSON document to enrich, a Parquet file to repartition. Each object can be processed independently — no coordination between tasks.

You want:

- **High throughput.** Dozens of objects in flight concurrently.
- **Bounded resources.** Not unbounded goroutines; not an exhausted connection pool.
- **Graceful shutdown.** When the timeout fires or SIGINT arrives, drain in-flight work and stop submitting.
- **Ordered results.** The final output reflects the order objects were submitted, not the order they happened to complete.
- **Error visibility.** When objects fail or time out, you want to know which ones and why.

## What this example shows

- **Bounded concurrency.** `WithMaxGoroutines` caps in-flight work. A submission beyond the limit blocks at `GoCtx` until a slot frees — natural backpressure with no semaphore code.
- **Context-aware submission and execution.** `WithContext(ctx)` derives the pool's task context from the run's context, so a timeout or SIGINT cancels in-flight tasks immediately — not just future ones. `GoCtx(ctx, ...)` also returns `ctx.Err()` if the context is cancelled while blocked waiting for a slot, so the submission loop exits cleanly without blocking indefinitely.
- **Per-task timeout.** `WithTaskTimeout` cancels the context delivered to each task after the deadline. The stub injects latencies of 10–300ms so roughly one-third of tasks time out at the default 200ms, appearing as `context deadline exceeded` in the live output.
- **Random failures.** The stub returns one of three domain errors (`object corrupted`, `checksum mismatch`, `storage quota exceeded`) on 20% of downloads.
- **Submission-order results.** Tasks complete in a different order than they were submitted — the live log makes this visible through jumping `idx` values. `ResultPool.Wait()` sorts the results back into submission order before returning, so the final summary is always sequential.
- **Error aggregation.** Every failed task's error is collected. `p.Errors()` returns them as `[]error`; `p.Wait()` returns `errors.Join` of all of them alongside the results.

## Running

```sh
go run . [flags]
```

| Flag            | Default | Effect                                                       |
| --------------- | ------- | ------------------------------------------------------------ |
| `-workers`      | `5`     | Raise to increase throughput; lower to see more backpressure |
| `-task-timeout` | `200ms` | Lower to see more timeouts; raise to let slow tasks complete |
| `-run-for`      | `5s`    | How long the pipeline runs before the context cancels        |

The live output shows tasks completing out of order. The summary at the end presents the same results in submission order with a count of ok / errored / timed-out.
