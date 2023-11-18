#![cfg(feature = "integration-test")]

use async_trait::async_trait;

use crate::storage::{
    RaftLogEntryRef, RaftStorageMonitorTrait, RaftStoragePersisterTrait,
    RaftStorageTrait, RaftStoredState,
};
use crate::{
    AppendEntriesArgs, AppendEntriesReply, Index, InstallSnapshotArgs,
    InstallSnapshotReply, RemoteRaft, RequestVoteArgs, RequestVoteReply, Term,
};

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

pub struct DoNothingRaftStorage;

impl RaftStorageTrait for DoNothingRaftStorage {
    type RaftStoragePersister<LogEntry: RaftLogEntryRef> =
        DoNothingRaftStoragePersister;
    type RaftStorageMonitor = DoNothingRaftStorageMonitor;

    fn persister<LogEntry>(
        self,
    ) -> std::sync::Arc<DoNothingRaftStoragePersister>
    where
        LogEntry: RaftLogEntryRef,
    {
        std::sync::Arc::new(DoNothingRaftStoragePersister)
    }

    fn read_state(&self) -> std::io::Result<RaftStoredState> {
        Ok(RaftStoredState {
            current_term: Term(0),
            voted_for: "".to_string(),
            log: vec![],
            snapshot_index: 0,
            snapshot_term: Term(0),
            snapshot: vec![],
        })
    }

    fn log_compaction_required(&self) -> bool {
        false
    }

    fn monitor(&self) -> DoNothingRaftStorageMonitor {
        DoNothingRaftStorageMonitor
    }
}

pub struct DoNothingRaftStorageMonitor;

impl RaftStorageMonitorTrait for DoNothingRaftStorageMonitor {
    fn should_compact_log_now(&self) -> bool {
        return false;
    }
}

pub struct DoNothingRaftStoragePersister;

impl<LogEntry: RaftLogEntryRef> RaftStoragePersisterTrait<LogEntry>
    for DoNothingRaftStoragePersister
{
    fn save_term_vote(&self, _term: Term, _voted_for: String) {}

    fn append_one_entry(&self, _entry: &LogEntry) {}

    fn append_entries<'a, LogEntryList>(&self, _entries: LogEntryList)
    where
        LogEntry: 'a,
        LogEntryList: IntoIterator<Item = &'a LogEntry>,
    {
    }

    fn update_snapshot(&self, _index: Index, _term: Term, _snapshot: &[u8]) {}
}
