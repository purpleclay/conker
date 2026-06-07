# Log Enrichment using `stream.Stream`

Demonstrates `stream.Stream`: enrich a continuous stream of web server log entries concurrently — resolving each client IP to a country — while preserving the original chronological order in the output.

## The use case

A web server emits a continuous stream of log entries. Each entry needs to be enriched with geographic information derived from the client IP address — the kind of lookup that involves a network call and has variable latency.

Each entry can be processed independently, so running enrichments in parallel is natural. The challenge is that the enriched log must stay in chronological order: reordering log lines makes them significantly harder to read and breaks tooling that relies on temporal sequence.

## What this example shows

- **Concurrent enrichment.** `WithMaxGoroutines` controls how many IP lookups run in parallel. Each producer performs the lookup and returns the enriched entry as a callback.
- **Strict submission order.** The dispatcher advances entry by entry in submission order — a fast lookup whose entry follows a slow one waits. The final table always reflects the original log sequence regardless of completion order.
- **Context-aware submission.** `GoCtx` returns `ctx.Err()` if the run timeout or SIGINT fires while blocked waiting for a slot, so the loop exits cleanly without blocking indefinitely.
- **Ordered output without buffering.** Unlike a pool-based approach that collects all results and sorts at the end, stream emits each entry as soon as its slot's turn arrives — output flows incrementally.

## Running

```sh
go run . [flags]
```

| Flag       | Default | Effect                                                                  |
| ---------- | ------- | ----------------------------------------------------------------------- |
| `-workers` | `5`     | Raise to increase enrichment throughput; lower to see more backpressure |
| `-run-for` | `5s`    | How long the pipeline runs before the context cancels                   |

The live output shows enrichments completing with jumping `idx` values (completion order). The summary at the end presents the same entries in their original submission sequence.
