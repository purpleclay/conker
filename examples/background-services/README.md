# Background Services using `daemon.Pool`

Demonstrates the `daemon` package: run a small set of long-lived background workers ("daemons") for a service, then shut them down gracefully when the run ends — either after `-run-for` elapses or on SIGINT.

## The use case

A service often has work that isn't request-shaped: periodic cache refreshes, metrics reporting, connection draining on shutdown. These run for the lifetime of the program, not for a single batch of tasks — the wrong fit for `pool.Pool`'s "submit work, then Wait" model.

You want:

- **Long-lived workers** that run until told to stop, not until a queue drains.
- **Coordinated shutdown** — a single signal cancels every daemon's context.
- **A bound on shutdown time**, so one slow daemon can't hang the whole process.
- **Panic isolation** — one daemon panicking shouldn't take down the others, and the failure should still be reported.

## What this example shows

- **Long-lived daemons.** `metricsReporter` and `cacheRefresher` are spawned with `d.Spawn` and run until their context is cancelled — they aren't one-shot tasks.
- **Coordinated cancellation.** `daemon.New(ctx)` derives every daemon's context from the run's context. When `-run-for` elapses or SIGINT arrives, all three daemons observe cancellation via the same `ctx.Done()`.
- **Panic isolation and reporting.** `cacheRefresher` panics on ~15% of refreshes (a "corrupted snapshot"). `daemon.Pool` recovers this — `metricsReporter` and `connection-drainer` keep running — and `Stop` reports it as `*panics.Recovered`, detectable via `errors.Is(err, panics.ErrPanic)`.
- **Bounded graceful shutdown.** `Stop(stopCtx)` waits for all daemons to return, bounded by `-shutdown-timeout`. With `-slow-drain`, `connection-drainer` takes longer than `-shutdown-timeout` to finish, so `Stop` returns `context.DeadlineExceeded` before draining completes — the daemon keeps running, but this example exits without waiting for it.

## Running

```sh
go run . [flags]
```

| Flag                | Default | Effect                                                            |
| ------------------- | ------- | ------------------------------------------------------------------ |
| `-run-for`          | `5s`    | How long the service runs before shutdown is requested           |
| `-shutdown-timeout` | `2s`    | Bound on how long `Stop` waits for daemons to return              |
| `-slow-drain`       | `false` | Make `connection-drainer` take 3s to drain — longer than the default `-shutdown-timeout`, demonstrating `Stop`'s deadline |

Try `-slow-drain` with the default `-shutdown-timeout=2s` (or shorter) to see `Stop` return `context.DeadlineExceeded` before `connection-drainer` finishes — the process exits without observing the drain complete. Run a few times without it to see `cache-refresher`'s occasional panic reported via `*panics.Recovered`.
