// Package daemon provides a pool of long-lived workers ("daemons") that run
// until told to stop, with graceful-shutdown semantics.
//
// Unlike [pool.Pool], which is suited to bursts of work followed by Wait,
// daemons run for the lifetime of the program — or until [Pool.Stop] is
// called. Create a Pool with [New] and start workers with [Pool.Spawn]. Each
// worker receives a context that is cancelled when [Pool.Stop] is called, or
// when the context passed to [New] is cancelled. [Pool.Stop] waits for every
// daemon — including those spawned recursively — to return, bounded by its
// own context, and propagates any recovered panics as *[panics.Recovered]
// values.
package daemon
