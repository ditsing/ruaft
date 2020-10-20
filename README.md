# Ruaft: Raft implemented in Rust

`Ruaft` is a Rust version of [the Raft consensus protocol](https://raft.github.io/). The name is clearly made up.

At the moment it is a gigantic class lives in a single file. It exposes 4 APIs, to create a Raft instance (`new()`),
start a new contract (`start()`), get the current status (`get_status()`) and shut it down (`kill()`). When a consensus
is reached, it notifies the clients via an `apply_command` callback. It saves its internal state to local disk via a
`persister`, which is supplied at creation.

There are also
two half-APIs: `process_append_entries()` and `process_request_vote()`, serving requests from other Raft instances of
the same setup.

The implementation is thoroughly tested. I copied the tests from an obvious source. To avoid being indexed by a search
engine, I will not name the source. The testing framework from the same source is also translated from the original Go
version. The code can be found at [`labrpc`](https://github.com/ditsing/labrpc) repo.

## Next steps
- [ ] Split into multiple files
- [ ] Add public documentation
- [ ] Add a proper RPC interface to all public methods
- [ ] Benchmarks
