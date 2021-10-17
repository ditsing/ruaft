# kvraft

`kvraft` is a distributed KV store. Both the keys and the values are strings. `Put`, `Get` and `Append` operations are
supported.

`kvraft` provides a consistent view of the managed data to each client. It relies on all clients to be nice
and to follow a certain protocol when talking to it. For example, each client should send no more than one request at the
same time. Clients are identified by a unique ID number. Each client must register its ID number before sending
requests. Multiple clients can co-exist within the same thread or process. There are no consistency guarantees between
different clients.

Clients can use the [client side library][client.rs] to talk to a `kvraft` cluster.

[client.rs]: https://github.com/ditsing/ruaft/blob/master/kvraft/src/client.rs
