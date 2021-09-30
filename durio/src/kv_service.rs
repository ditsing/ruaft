use std::sync::Arc;

use tarpc::context::Context;

use kvraft::{GetArgs, GetReply, KVServer, PutAppendArgs, PutAppendReply};

#[tarpc::service]
trait KVService {
    async fn get(args: GetArgs) -> GetReply;
    async fn put_append(args: PutAppendArgs) -> PutAppendReply;
}

#[derive(Clone)]
struct KVRpcServer(Arc<KVServer>);

#[tarpc::server]
impl KVService for KVRpcServer {
    async fn get(self, _context: Context, args: GetArgs) -> GetReply {
        self.0.get(args).await
    }

    async fn put_append(
        self,
        _context: Context,
        args: PutAppendArgs,
    ) -> PutAppendReply {
        self.0.put_append(args).await
    }
}
