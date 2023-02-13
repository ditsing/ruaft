use async_trait::async_trait;
use bytes::Bytes;

use crate::{
    AppendEntriesArgs, AppendEntriesReply, InstallSnapshotArgs,
    InstallSnapshotReply, Persister, RemoteRaft, RequestVoteArgs,
    RequestVoteReply,
};

#[derive(Clone)]
pub struct DoNothingPersister;
impl Persister for DoNothingPersister {
    fn read_state(&self) -> Bytes {
        Bytes::new()
    }

    fn save_state(&self, _bytes: Bytes) {}

    fn state_size(&self) -> usize {
        0
    }

    fn save_snapshot_and_state(&self, _: Bytes, _: &[u8]) {}
}

#[derive(Clone)]
pub struct DoNothingRemoteRaft;
#[async_trait]
impl<Command: 'static + Send> RemoteRaft<Command> for DoNothingRemoteRaft {
    async fn request_vote(
        &self,
        _args: RequestVoteArgs,
    ) -> std::io::Result<RequestVoteReply> {
        unimplemented!()
    }

    async fn append_entries(
        &self,
        _args: AppendEntriesArgs<Command>,
    ) -> std::io::Result<AppendEntriesReply> {
        unimplemented!()
    }

    async fn install_snapshot(
        &self,
        _args: InstallSnapshotArgs,
    ) -> std::io::Result<InstallSnapshotReply> {
        unimplemented!()
    }
}
