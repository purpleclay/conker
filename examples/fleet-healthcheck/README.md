# Fleet Health Check using `iter`

Concurrently health-check a fixed fleet of hosts using `iter.MapSeqErr`, returning results in the original fleet order regardless of completion order.

## The use case

You have a known, bounded set of things to process — a fleet of hosts, a directory of files, a batch of records already loaded into memory. Unlike a continuous stream, the full collection is available up front as a slice, map, or `iter.Seq`.

You want:

- **Concurrent processing** of the collection, bounded by a worker limit.
- **Results in the original order**, so the report reads the same way the input was written.
- **Per-call context propagation**, so a deadline or cancellation reaches in-flight work.
- **Every result, even on error** — a failed check shouldn't make its neighbours disappear from the report.

## What this example shows

- **Bounded, ordered concurrency.** `iter.MapSeqErr` walks `slices.Values(fleet)`, running up to `WithMaxGoroutines` checks at a time, and returns results in fleet order — the live log shows hosts completing out of sequence, but the summary table doesn't.
- **Derived, per-call context.** Each check receives a context derived from the run's governing context (`WithContext`). When the overall `-timeout` expires or SIGINT arrives, in-flight checks observe cancellation through that context.
- **Zero-value contract on error.** A failed check contributes a zero-value `HealthResult` to the ordered output; the example labels each row using the original `fleet` slice (which it owns) rather than relying on the result.
- **Fail-fast cancellation.** The fleet's `auth-03.internal` always returns a fatal DNS error. With `-fail-fast`, `WithCancelOnError` cancels the derived context for all in-flight checks and stops dispatching new ones the moment that error occurs — the summary shows the remaining hosts as skipped.
- **Error aggregation.** Every failed check's error is collected and returned via `errors.Join`, printed at the end of the run.

## Running

```sh
go run . [flags]
```

| Flag         | Default | Effect                                                                |
| ------------ | ------- | ---------------------------------------------------------------------- |
| `-workers`   | `5`     | Raise to increase throughput; lower (e.g. `1`) to make ordering obvious |
| `-timeout`   | `5s`    | Overall deadline for the run                                          |
| `-fail-fast` | `false` | Cancel remaining checks as soon as `auth-03.internal`'s fatal error occurs |

The live output shows checks completing out of order. The summary table at the end presents every host in its original fleet order, with a count of hosts checked and any aggregated errors.
