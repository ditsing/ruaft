use async_trait::async_trait;

use crate::common::{
    CommitSentinelArgs, CommitSentinelReply, GetArgs, GetReply, PutAppendArgs,
    PutAppendReply,
};

#[async_trait]
pub trait RemoteKvraft: Send + Sync + 'static {
    async fn get(&self, args: GetArgs) -> std::io::Result<GetReply>;

    async fn put_append(
        &self,
        args: PutAppendArgs,
    ) -> std::io::Result<PutAppendReply>;

    async fn commit_sentinel(
        &self,
        args: CommitSentinelArgs,
    ) -> std::io::Result<CommitSentinelReply>;
}
