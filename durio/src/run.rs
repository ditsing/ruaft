use std::net::SocketAddr;
use std::sync::Arc;

use kvraft::KVServer;

use crate::kv_service::start_kv_service_server;
use crate::raft_service::{start_raft_service_server, LazyRaftServiceClient};
use crate::storage::DoNothingRaftStorage;

pub(crate) async fn run_kv_instance(
    addr: SocketAddr,
    raft_peers: Vec<SocketAddr>,
    me: usize,
) -> std::io::Result<Arc<KVServer>> {
    let local_raft_peer = raft_peers[me];

    let mut remote_rafts = vec![];
    for raft_peer in raft_peers {
        remote_rafts.push(LazyRaftServiceClient::new(raft_peer));
    }

    let storage = DoNothingRaftStorage::default();

    let kv_server = KVServer::new(remote_rafts, me, storage);
    let raft = kv_server.raft().clone();

    start_raft_service_server(local_raft_peer, raft).await?;
    start_kv_service_server(addr, kv_server.clone()).await?;

    Ok(kv_server)
}
