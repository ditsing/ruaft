use std::any::Any;
use std::cell::RefCell;

use crate::remote_peer::RemotePeer;
use crate::term_marker::TermMarker;
use crate::verify_authority::DaemonBeatTicker;
use crate::{Peer, RemoteRaft};

/// A struct that contains useful utilities when contacting peers.
///
/// The motivation of RemoteContext is to avoid cloning the `Arc<>` to utilities
/// multiple times for each request. Instead, we embed shared utilities in the
/// thread local space of each thread in a thread pool. Those references will be
/// released when the thread pool is destroyed.
///
/// This class provides a static reference to those utilities, which also helps
/// simplifying lifetimes in futures.
///
/// A big hack in this class is that we need to store generic structs in a
/// thread local environment. This is because that the RPC interface is generic
/// over the data to store in Raft. Thus `RemoteContext` must be generic, too.
///
/// Generic thread local variables is not supported in Rust, due to the fact
/// that one could potentially store multiple instances of the same class with
/// different generic parameters. The instances must be somehow identified by
/// their concrete types, which greatly increases implementation complexity.
///
/// Luckily in our case, the "multiple instance" issue does not apply. A thread
/// pool belongs to a known Raft instance, which has one set of known generic
/// parameters. We would only ever need to store one instance of RemoteContext
/// in one thread. Thus the ambiguity is casted away in `fetch_context()`.
#[derive(Clone)]
pub(crate) struct RemoteContext<Command> {
    term_marker: TermMarker<Command>,
    remote_peers: Vec<RemotePeer<Command, Peer>>,
}

impl<Command: 'static> RemoteContext<Command> {
    pub fn create(
        term_marker: TermMarker<Command>,
        remote_peers: Vec<RemotePeer<Command, Peer>>,
    ) -> Self {
        Self {
            term_marker,
            remote_peers,
        }
    }

    pub fn term_marker() -> &'static TermMarker<Command> {
        &Self::fetch_context().term_marker
    }

    pub fn remote_peer(peer: Peer) -> &'static RemotePeer<Command, Peer> {
        &Self::fetch_context().remote_peers[peer.0]
    }

    pub fn rpc_client(peer: Peer) -> &'static dyn RemoteRaft<Command> {
        Self::remote_peer(peer).rpc_client.as_ref()
    }

    pub fn beat_ticker(peer: Peer) -> &'static DaemonBeatTicker {
        &Self::remote_peer(peer).beat_ticker
    }

    thread_local! {
        // Using Any to mask the fact that we are storing a generic struct.
        static REMOTE_CONTEXT: RefCell<Option<&'static dyn Any>> = RefCell::new(None);
    }

    pub fn attach(self) {
        Self::set_context(Box::new(self))
    }

    pub fn detach() -> Box<Self> {
        let static_context = Self::fetch_context();
        Self::REMOTE_CONTEXT.with(|context| context.borrow_mut().take());
        unsafe { Box::from_raw((static_context as *const Self) as *mut Self) }
    }

    fn set_context(context: Box<Self>) {
        let context_ref = Box::leak(context);
        let any_ref: &'static mut dyn Any = context_ref;
        Self::REMOTE_CONTEXT
            .with(|context| *context.borrow_mut() = Some(any_ref));
    }

    fn fetch_context() -> &'static Self {
        let any_ref = Self::REMOTE_CONTEXT.with(|context| *context.borrow());
        let Some(any_ref) = any_ref else {
            panic!("Context is not set");
        };

        any_ref
            .downcast_ref::<Self>()
            .expect("Context is set to the wrong type.")
    }
}

#[cfg(test)]
mod tests {
    use std::panic::catch_unwind;
    use std::sync::Arc;

    use async_trait::async_trait;
    use bytes::Bytes;
    use parking_lot::Mutex;

    use crate::election::ElectionState;
    use crate::remote_peer::RemotePeer;
    use crate::term_marker::TermMarker;
    use crate::verify_authority::VerifyAuthorityDaemon;
    use crate::{
        AppendEntriesArgs, AppendEntriesReply, InstallSnapshotArgs,
        InstallSnapshotReply, Peer, Persister, RaftState, RemoteRaft,
        RequestVoteArgs, RequestVoteReply,
    };

    use super::RemoteContext;

    struct DoNothingPersister;
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

    struct DoNothingRemoteRaft;
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

    #[test]
    fn test_context_api() {
        let rf = Arc::new(Mutex::new(RaftState::<i32>::create(1, Peer(0))));
        let election = Arc::new(ElectionState::create());
        let verify_authority_daemon = VerifyAuthorityDaemon::create(1);
        let term_marker =
            TermMarker::create(rf, election, Arc::new(DoNothingPersister));
        let remote_peer = RemotePeer::create(
            Peer(0),
            DoNothingRemoteRaft,
            verify_authority_daemon.beat_ticker(0),
        );

        let context =
            Box::new(RemoteContext::create(term_marker, vec![remote_peer]));
        let context_ptr: *const RemoteContext<i32> = &*context;

        RemoteContext::set_context(context);

        let fetched_context = RemoteContext::fetch_context();
        let fetched_context_ptr: *const RemoteContext<i32> = fetched_context;
        assert_eq!(context_ptr, fetched_context_ptr);

        let detached_context = RemoteContext::detach();
        let detached_context_ptr: *const RemoteContext<i32> =
            &*detached_context;
        assert_eq!(context_ptr, detached_context_ptr);

        catch_unwind(|| {
            RemoteContext::<i32>::fetch_context();
        })
        .expect_err("Expecting error 'Context is not set'");
    }
}
