# durio

`durio` is a distributed KV store that comes with a simple HTTP interface. I run it on Raspberry Pis. In this repo you
can find scripts ([`build.sh`][build.sh], [`curl.sh`][curl.sh]) and notes ([`pi-setup-notes`][pi-setup-notes.md]) to
help build, run and test `durio` on Raspberry Pis.

By default `durio` has 3 replicas. Each replica has a web server, a [KV server][kvraft] and a [raft][raft] instance
behind it. The 3 web servers talk to all KV servers, while each KV server only talks to the raft instance in the same
process.

`durio` relies on [`warp`][warp] to provide the web server, and [`tarpc`][tarpc] to run the RPC services.

## Clients

Clients connected to one web server get a consistent (linearizable) view of the data stored in the KV store. The web 
servers can accept multiple incoming requests. At any given time, at most one request will be sent to the leading KV
server. That limits the amount of data one web server can process.

[durio]: https://github.com/ditsing/ruaft/tree/master/durio
[build.sh]: https://github.com/ditsing/ruaft/tree/master/durio/build.sh
[curl.sh]: https://github.com/ditsing/ruaft/tree/master/durio/curl.sh
[pi-setup-notes.md]: https://github.com/ditsing/ruaft/tree/master/durio/pi-setup-notes.md
[kvraft]: https://github.com/ditsing/ruaft/tree/master/kvraft
[raft]: https://github.com/ditsing/ruaft
[tarpc]: https://github.com/google/tarpc
[warp]: https://github.com/seanmonstar/warp
