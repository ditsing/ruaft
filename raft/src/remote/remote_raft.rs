use async_trait::async_trait;

use crate::{
    AppendEntriesArgs, AppendEntriesReply, InstallSnapshotArgs,
    InstallSnapshotReply, RequestVoteArgs, RequestVoteReply,
};

#[async_trait]
pub trait RemoteRaft<Command>: Send + Sync {
    async fn request_vote(
        &self,
        args: RequestVoteArgs,
    ) -> std::io::Result<RequestVoteReply>;

    async fn append_entries(
        &self,
        args: AppendEntriesArgs<Command>,
    ) -> std::io::Result<AppendEntriesReply>;

    async fn install_snapshot(
        &self,
        args: InstallSnapshotArgs,
    ) -> std::io::Result<InstallSnapshotReply>;
}

#[async_trait]
impl<Command, R> RemoteRaft<Command> for R
where
    Command: Send + 'static,
    R: AsRef<dyn RemoteRaft<Command>> + Send + Sync,
{
    async fn request_vote(
        &self,
        args: RequestVoteArgs,
    ) -> std::io::Result<RequestVoteReply> {
        self.as_ref().request_vote(args).await
    }

    async fn append_entries(
        &self,
        args: AppendEntriesArgs<Command>,
    ) -> std::io::Result<AppendEntriesReply> {
        self.as_ref().append_entries(args).await
    }

    async fn install_snapshot(
        &self,
        args: InstallSnapshotArgs,
    ) -> std::io::Result<InstallSnapshotReply> {
        self.as_ref().install_snapshot(args).await
    }
}
