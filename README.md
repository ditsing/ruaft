# Ruaft: Raft implemented in Rust

Ruaft is a Rust version of [the Raft consensus protocol](https://raft.github.io/).

## Raft

Raft is the algorithm behind the famous key-value store [etcd](https://github.com/etcd-io/etcd). It helps replicas of a
distributed application to reach consensus on internal state. Raft maintains a log that consists of entries. Each entry
carries one piece of data (commonly referred to as a "command") for the application. Once an entry is committed to the
log, it will never be changed or removed across all replicas. Entries are appended to the end of the log linearly, which
guarantees that the log never diverges.

## APIs

Each application replica has a Raft instance that runs side by side with it. To add a log entry to the Raft log, the
application calls `start()` with the data it wants to store. The log entry is not committed immediately. Instead, when
the Raft instance is sure that the log entry is agreed upon, it calls the application back via an `apply_command`
callback, which is supplied by the application.

Internally, Raft talks to other replicas of the same application via an RPC interface. From time to time, Raft saves its
log (and other data) to a permanent storage via a `persister`. The log can grow without bound. To save storage space,
Raft periodically asks the application to take a snapshot of its state. Log entries contained in the snapshot will be
discarded.

An application creates a Raft instance with `new()`, with RPC interfaces for communication, a `persister`,
a `apply_command`
callback, and a `request_snapshot` callback. More details of those callbacks can be found in the comment of the modules.

### Raft RPCs

Ruaft has three RPC handlers: `process_append_entries()`, `process_request_vote()` and
`process_install_snapshot()`, serving requests from other Raft instances of the same application. These RPC handlers
should be added to the RPC system that the application uses.

### Graceful shutdown

The `kill()` method provides a clean way to gracefully shutdown a Ruaft instance. It notifies all threads and wait for
all tasks to complete. `kill()` then checks if there are any panics or assertion failures during the execution. It
panics the main thread if there is any error. Otherwise `kill()` is guaranteed to return, assuming there is no thread
starvation.

## Code Quality

The implementation is thoroughly tested. I copied (and translated) the tests from an obvious source. To avoid being
indexed by a search engine, I will not name the source. The testing framework from the same source is also translated
from the original Go version. The code can be found at the [`labrpc`](https://github.com/ditsing/labrpc) repo.

## KV Server

To test the snapshot functionality, I wrote a key-value store that supports `get()`, `put()` and `append()`. The
complexity is so high that it has its own set of tests. Integration tests in `tests/snapshot_tests.rs` are all based on
the KV server. The KV server is inspired by the equivalent Go version.

## Daemons

Ruaft uses both threads and async thread pools. There are 4 'daemon threads':

1. Election timer: watches the election timer, starts and cancels elections. Correctly implementing a versioned timer is
   one of the most difficult tasks in Ruaft;
1. Sync log entries: waits for new logs, talks to followers and marks contracts as 'agreed on';
1. Apply command daemon: sends consensus to the application. Communicates with the rest of `Ruaft` via a `Condvar`;
1. Snapshot daemon: requests and processes snapshots from the application. Communicates with the rest of `Ruaft` via
   A `Condvar` and a thread parker. Unlike other daemons, the snapshot daemon runs even if the current instance is a
   follower.

To avoid blocking, daemon threads never sends RPC directly. RPC-sending is offloaded to a dedicated thread pool that
supports `async/.await`. There is a global RPC timeout, so RPCs never block forever. The thread pool also handles
vote-counting in the elections, given the massive amount of waiting involved.

Last but not least, there is the heartbeat daemon. It is so simple that it is just a list of periodical tasks that run
forever in the thread pool.

## Running

It is close to impossible to run Ruaft outside of the testing setup under `tests`. One would have to supply an RPC
environment plus bridges, a `persister`, an `apply_command` callback and a `request_snapshot` callback.

Things would be better after I implement an RPC interface and improve the `persister` trait.

## Next steps

- [x] Split into multiple files
- [x] Add public documentation
- [ ] Add a proper RPC interface to all public methods
- [ ] Benchmarks
- [x] Allow storing arbitrary information
- [ ] Add more logging.