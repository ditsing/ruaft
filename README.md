# Ruaft: Raft implemented in Rust

Ruaft is a Rust version of [the Raft consensus protocol](https://raft.github.io/). The name is clearly made up.

Ruaft exposes 4 APIs, to create a Raft instance (`new()`), start a new contract (`start()`), get the current status
(`get_state()`) and shut it down (`kill()`). When a consensus is reached, it notifies the clients via an
`apply_command` callback. It saves the internal state to local disks via a `persister`, which is supplied at creation.
Periodically, it asks the application to take a snapshot of the current state, and archives contracts included in the
snapshot to save memory and disk space.

There are also three internal RPC handlers: `process_append_entries()`, `process_request_vote()` and
`process_install_snapshot`, serving requests from other Raft instances of the same setup.

## Testing
The implementation is thoroughly tested. I copied (and translated) the tests from an obvious source. To avoid being
indexed by a search engine, I will not name the source. The testing framework from the same source is also translated
from the original Go version. The code can be found at the [`labrpc`](https://github.com/ditsing/labrpc) repo.

## Application: KV Server
To test the snapshot functionality, I wrote a KV server that supports `get()`, `put()` and `append()`. The complexity
of the KV server is so high that it has its own set of tests. Integration tests in `tests/snapshot_tests.rs` are all
based on the KV server. The KV server is inspired by the equivalent Go version.

## Threads
Ruaft uses both threads and async thread pools. There are 4 'daemon threads':

1. Election timer: watches the election timer, starts and cancels elections. Correctly implementing a versioned timer
   is one of the most difficult tasks in Ruaft;
1. Sync log entries: waits for new logs, talks to followers and marks contracts as 'agreed on';
1. Apply command daemon: sends consensus to the application. Communicates with the rest of `Ruaft` via a `Condvar`;
1. Snapshot daemon: requests and processes snapshots from the application. Communicates with the rest of `Ruaft` via
A `Condvar` and a thread parker. Unlike other daemons, the snapshot daemon runs even if the current instance is a
   follower.

To avoid blocking, daemon threads never sends RPC directly. RPC-handling is offloaded to a dedicated thread pool that
supports `async/.await`. There is a global RPC timeout, so RPCs never block forever. The thread pool also handles
vote-counting in the elections, given the massive amount of waiting involved.

Last but not least, the heartbeat daemon is so simple that it is just a list of periodical tasks that live in the
thread pool. It does not need its own thread.

## Shutdown
The `kill()` method provides a clean way to shutdown a Ruaft instance. It notifies all threads and wait for all tasks to
complete. `kill()` then checks if there are any panics or assertion failures during the execution. It panics the main
thread if there is any error. Otherwise `kill()` is guaranteed to return, assuming there is no thread starvation.

## Running
It is close to impossible to run Ruaft outside of the testing setup under `tests`. One would have to supply an RPC
environment plus bridges, a `persister`, an `apply_command` callback and a `request_snapshot` callback.

Things would be better after I implement an RPC interface and improve the `persister` trait.

## Next steps
- [x] Split into multiple files
- [ ] Add public documentation
- [ ] Add a proper RPC interface to all public methods
- [ ] Benchmarks
- [x] Allow storing of arbitrary information, instead of a `i32`
- [ ] Add more logging.