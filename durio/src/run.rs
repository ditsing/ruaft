use std::net::SocketAddr;
use std::sync::Arc;

use kvraft::KVServer;

use crate::kv_service::start_kv_service_server;
use crate::persister::Persister;
use crate::raft_service::{
    connect_to_raft_service, no_raft_service, start_raft_service_server,
};

pub(crate) async fn run_kv_instance(
    addr: SocketAddr,
    raft_peers: Vec<SocketAddr>,
    me: usize,
) -> std::io::Result<Arc<KVServer>> {
    let mut remote_rafts = vec![];
    for (index, raft_peer) in raft_peers.iter().enumerate() {
        let remote_raft = if index == me {
            no_raft_service()
        } else {
            connect_to_raft_service(*raft_peer).await?
        };
        remote_rafts.push(remote_raft);
    }

    let persister = Arc::new(Persister::new());

    let kv_server = KVServer::new(remote_rafts, me, persister, None);
    let raft = Arc::new(kv_server.raft().clone());

    start_raft_service_server(raft_peers[me], raft).await?;
    start_kv_service_server(addr, kv_server.clone()).await?;

    Ok(kv_server)
}
