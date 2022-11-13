use std::sync::Arc;

use crate::verify_authority::DaemonBeatTicker;
use crate::RemoteRaft;

#[derive(Clone)]
pub(crate) struct RemotePeer<Command, UniqueId> {
    #[allow(dead_code)]
    pub unique_id: UniqueId,
    pub rpc_client: Arc<dyn RemoteRaft<Command>>,
    pub beat_ticker: DaemonBeatTicker,
}

impl<Command, UniqueId> RemotePeer<Command, UniqueId> {
    pub fn create<RpcClient: 'static + RemoteRaft<Command>>(
        unique_id: UniqueId,
        rpc_client: RpcClient,
        beat_ticker: DaemonBeatTicker,
    ) -> Self {
        let rpc_client = Arc::new(rpc_client);
        RemotePeer {
            unique_id,
            rpc_client,
            beat_ticker,
        }
    }
}
