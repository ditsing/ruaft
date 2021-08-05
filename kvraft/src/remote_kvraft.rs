use async_trait::async_trait;

use crate::common::{GetArgs, GetReply, PutAppendArgs, PutAppendReply};

#[async_trait]
pub trait RemoteKvraft: Send + Sync + 'static {
    async fn get(&self, args: GetArgs) -> std::io::Result<GetReply>;

    async fn put_append(
        &self,
        args: PutAppendArgs,
    ) -> std::io::Result<PutAppendReply>;
}
