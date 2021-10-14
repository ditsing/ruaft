# Ruaft: Raft implemented in Rust

Ruaft is a Rust version of [the Raft consensus protocol](https://raft.github.io/).

## Raft

Raft is the algorithm behind the famous key-value store [etcd](https://github.com/etcd-io/etcd). The algorithm helps
replicas of a distributed application to reach consensus on its internal state. Raft maintains an array of entries,
collectively known as the log. Each entry of the log carries one piece of data for the application. Once an entry is
committed to the log, it will never be changed or removed. Entries are appended to the end of the log linearly, which
guarantees that the log never diverges across replicas. The log as a whole, represents the internal state of the
application. Replicas of the same application agree on the log entries, and thus agree on the internal state.

## Proof of Concept

[`durio`][durio] is a web-facing distributed key-value store. It is built
on top of Ruaft as a showcase application. It provides a set of JSON APIs to read and write key-value pairs
consistently across replicas.

I have a 3-replica durio service running on Raspberry Pis. See the [README][durio] for more details.

## Raft APIs

Each application replica has a Raft instance that runs side by side with it. To add a log entry to the Raft log, the
application calls `start()` with the data it wants to store (commonly referred to as a "command"). The log entry is not
committed immediately. Instead, when the Raft instance is sure that the log entry is agreed upon and committed, it calls
the application back via an `apply_command` callback, which is supplied by the application.

Internally, Raft talks to other replicas of the same application via an RPC interface. From time to time, Raft saves its
log (and other data) to a permanent storage via a `persister`. The log can grow without bound. To save storage space,
Raft periodically asks the application to take a snapshot of its internal state. Log entries contained in the snapshot
will be discarded.

## Building on top of Ruaft

To use Ruaft as a backend, an application would need to provide

1. A set of RPC clients that allows one Raft instance to send RPCs to other Raft instances. RPC
   clients should implement the [`RemoteRaft`][RemoteRaft] trait.
2. A `persister` that writes binary blobs to permanent storage. It must implement the [`Persister`][Persister] trait.
3. An `apply_command` function that accepts commands to the application's internal state. The commands must be applied
   to the application's internal state in order and sequentially.
4. Optionally, a `request_snapshot` function that takes snapshots on the application's internal state. Snapshots should
   be delivered to [`Raft::save_snapshot()`][save_snapshot] API.

All the above should be passed to the [Raft constructor][lib]. The constructor returns a Raft instance that is `Send`,
`Sync` and implements `Clone`. The Raft instance can be used by the application, as well as in the RPC server that is
about to be mentioned.

For each replica, the application must also run an RPC server that accepts the RPCs defined in
[`RemoteRaft`][RemoteRaft] (`append_entries()`, `request_vote()` and `install_snapshot()`). RPC requests should be
proxied to the corresponding API of the Raft instance. RPC clients provided by the application must be able to talk to
all RPC servers run in different replicas.

In sub-crates [durio][durio] and the underlying [kvraft][kvraft], you can find examples of all the elements mentioned
above. [`tarpc`][tarpc] is used to generate the RPC servers and clients. The `persister` does not do anything.
`apply_command` and `request_snapshot` is provided by [kvraft][kvraft].

## Threading Model

Ruaft uses both threads and async thread pools. There are four 'daemon threads' that runs latency-sensitive tasks:

1. Election timer: watches the election timer, starts and cancels elections. Correctly implementing a versioned timer is
   one of the most difficult tasks in Ruaft;
1. Sync log entries: waits for new logs, talks to followers and marks contracts as 'agreed on';
1. Apply command daemon: sends consensus to the application. Communicates with the rest of Ruaft via a `Condvar`;
1. Snapshot daemon: requests and processes snapshots from the application. Communicates with the rest of `Ruaft` via
   A `Condvar` and a thread parker. The snapshot daemon runs even if the current instance is a follower.

To avoid blocking, daemon threads never send RPCs directly. RPC-sending is offloaded to a dedicated thread pool that
supports `async/.await`. There is a global RPC timeout, so RPCs never block forever. The thread pool also handles
vote-counting in the elections, given the massive amount of waiting involved.

Last but not least, there is the heartbeat daemon. It is so simple that it is just a list of periodical tasks that run
forever in the thread pool.

### Why both?

We use daemon threads to minimise latency. Take the election timer as an example. The election timer is supposed to wake
up roughly every 150 milliseconds and check if a heartbeat has been received in that time. It only needs a tiny bit of
CPU, but we could not afford to delay its execution. The longer it takes for the timer to respond to a lost heartbeat,
the longer the pack runs without a leader. Having a standalone thread reserved for the election timer reduces the
delay to react.

Daemon threads are also used for isolation. Calling client callbacks (`apply_command` and `request_snapshot`) are
dangerous because they can block at any time. For that reason we should not run them in the shared thread pool. Instead,
one thread is created for each of the two callbacks. This way, we give more flexibility to the application
implementation, allowing one or both callbacks to be blocking.

On the other hand, thread pools are for throughput. Tasks that involve a lot of waiting, e.g. sending packets over the
network, are run on thread pools. We could send as many packets out as possible, while simultaneously waiting for the
responses to arrive. To get even more throughput, tasks sent to the same peer can be grouped together. Optimizations
like that will be added if proven useful.

## Code Quality

The implementation is thoroughly tested. I copied (and translated) the test sets from an obvious source. To avoid being
indexed by a search engine, I will not name the source. The testing framework from the same source is also translated
from the original Go version. The code can be found at the [`labrpc`][labrpc] repo.

### KV Server

To test the snapshot functionality, I wrote a key-value store that supports `get()`, `put()` and `append()`. The
complexity of the key-value store is so high that it has its own set of tests. For Ruaft, integration tests in
[`tests/snapshot_tests.rs`][snapshot_tests] are all based on the KV server. The KV server is inspired by the equivalent
Go version. See the [README][kvraft] for more.

### Graceful shutdown

The `kill()` method provides a clean way to gracefully shut down a Ruaft instance. It notifies all threads and wait for
all tasks to exit. `kill()` then checks if there are any panics or assertion failures during the lifetime of the Raft
instance. It panics the main thread if there is any error. If there is no failure, `kill()` is guaranteed to return,
assuming there is no thread starvation.

## Next steps

- [x] Split the code into multiple files
- [x] Add public documentation
- [x] Add a proper RPC interface to all public methods
- [x] Allow storing arbitrary information
- [x] Add more logging
- [ ] Benchmarks
- [ ] Support the `Prevote` state
- [x] Run Ruaft on a Raspberry Pi cluster

[durio]: https://github.com/ditsing/ruaft/tree/master/durio
[kvraft]: https://github.com/ditsing/ruaft/tree/master/kvraft
[labrpc]: https://github.com/ditsing/librpc
[tarpc]: https://github.com/google/tarpc
[lib]: https://github.com/ditsing/ruaft/blob/master/src/lib.rs
[RemoteRaft]: https://github.com/ditsing/ruaft/blob/master/src/remote_raft.rs
[Persister]: https://github.com/ditsing/ruaft/blob/master/src/persister.rs
[save_snapshot]: https://github.com/ditsing/ruaft/blob/master/src/snapshot.rs
[snapshot_tests]: https://github.com/ditsing/ruaft/blob/master/tests/snapshot_tests.rs
