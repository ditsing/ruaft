use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use crossbeam_channel::{Receiver, Sender};
use once_cell::sync::OnceCell;

use kvraft::{
    GetArgs, KVServer, PutAppendArgs, PutAppendEnum, UniqueId, UniqueKVOp,
};
use ruaft::{
    AppendEntriesArgs, AppendEntriesReply, InstallSnapshotArgs,
    InstallSnapshotReply, Raft, RemoteRaft, ReplicableCommand, RequestVoteArgs,
    RequestVoteReply,
};

use crate::Persister;

type RaftId = usize;

pub struct EventHandle {
    pub from: RaftId,
    pub to: RaftId,
    sender: futures_channel::oneshot::Sender<std::io::Result<()>>,
}

struct EventStub {
    receiver: futures_channel::oneshot::Receiver<std::io::Result<()>>,
}

fn create_event_pair(from: RaftId, to: RaftId) -> (EventHandle, EventStub) {
    let (sender, receiver) = futures_channel::oneshot::channel();
    (EventHandle { from, to, sender }, EventStub { receiver })
}

impl EventHandle {
    pub fn unblock(self) {
        self.sender.send(Ok(())).unwrap();
    }

    pub fn reply_error(self, e: std::io::Error) {
        self.sender.send(Err(e)).unwrap();
    }

    pub fn reply_interrupted_error(self) {
        self.reply_error(std::io::Error::from(std::io::ErrorKind::Interrupted))
    }
}

impl EventStub {
    pub async fn wait(self) -> std::io::Result<()> {
        self.receiver.await.unwrap_or(Ok(()))
    }
}

pub enum RaftRpcEvent<T> {
    RequestVoteRequest(RequestVoteArgs),
    RequestVoteResponse(RequestVoteArgs, RequestVoteReply),
    AppendEntriesRequest(AppendEntriesArgs<T>),
    AppendEntriesResponse(AppendEntriesArgs<T>, AppendEntriesReply),
    InstallSnapshotRequest(InstallSnapshotArgs),
    InstallSnapshotResponse(InstallSnapshotArgs, InstallSnapshotReply),
}

struct InterceptingRpcClient<T> {
    from: RaftId,
    to: RaftId,
    target: OnceCell<Raft<T>>,
    event_queue: Sender<(RaftRpcEvent<T>, EventHandle)>,
}

impl<T> InterceptingRpcClient<T> {
    async fn intercept(&self, event: RaftRpcEvent<T>) -> std::io::Result<()> {
        let (handle, stub) = create_event_pair(self.from, self.to);
        let _ = self.event_queue.send((event, handle));
        stub.wait().await
    }

    pub fn set_raft(&self, target: Raft<T>) {
        self.target
            .set(target)
            .map_err(|_| ())
            .expect("Raft should only be set once");
    }
}

#[async_trait]
impl<T: ReplicableCommand> RemoteRaft<T> for &InterceptingRpcClient<T> {
    async fn request_vote(
        &self,
        args: RequestVoteArgs,
    ) -> std::io::Result<RequestVoteReply> {
        let event_result = self
            .intercept(RaftRpcEvent::RequestVoteRequest(args.clone()))
            .await;
        if let Err(e) = event_result {
            return Err(e);
        };

        let reply = self.target.wait().process_request_vote(args.clone());

        self.intercept(RaftRpcEvent::RequestVoteResponse(args, reply.clone()))
            .await
            .map(|_| reply)
    }

    async fn append_entries(
        &self,
        args: AppendEntriesArgs<T>,
    ) -> std::io::Result<AppendEntriesReply> {
        let args_clone = args.clone();
        let event_result = self
            .intercept(RaftRpcEvent::AppendEntriesRequest(args_clone))
            .await;
        if let Err(e) = event_result {
            return Err(e);
        };

        let reply = self.target.wait().process_append_entries(args.clone());

        self.intercept(RaftRpcEvent::AppendEntriesResponse(args, reply.clone()))
            .await
            .map(|_| reply)
    }

    async fn install_snapshot(
        &self,
        args: InstallSnapshotArgs,
    ) -> std::io::Result<InstallSnapshotReply> {
        let event_result = self
            .intercept(RaftRpcEvent::InstallSnapshotRequest(args.clone()))
            .await;
        if let Err(e) = event_result {
            return Err(e);
        };

        let reply = self.target.wait().process_install_snapshot(args.clone());

        self.intercept(RaftRpcEvent::InstallSnapshotResponse(
            args,
            reply.clone(),
        ))
        .await
        .map(|_| reply)
    }
}

pub struct EventQueue<T> {
    pub receiver: Receiver<(RaftRpcEvent<T>, EventHandle)>,
}

fn make_grid_clients<T>(
    server_count: usize,
) -> (EventQueue<T>, Vec<Vec<InterceptingRpcClient<T>>>) {
    let (sender, receiver) = crossbeam_channel::unbounded();
    let mut all_clients = vec![];
    for from in 0..server_count {
        let mut clients = vec![];
        for to in 0..server_count {
            let interceptor = InterceptingRpcClient {
                from,
                to,
                target: Default::default(),
                event_queue: sender.clone(),
            };
            clients.push(interceptor);
        }
        all_clients.push(clients);
    }
    (EventQueue { receiver }, all_clients)
}

pub struct Config {
    pub event_queue: EventQueue<UniqueKVOp>,
    pub kv_servers: Vec<Arc<KVServer>>,
    seq: AtomicUsize,
}

impl Config {
    pub fn find_leader(&self) -> Option<&KVServer> {
        let start = Instant::now();
        while start.elapsed() < Duration::from_secs(1) {
            if let Some(kv_server) = self
                .kv_servers
                .iter()
                .find(|kv_server| kv_server.raft().get_state().1)
            {
                return Some(kv_server.as_ref());
            }
        }
        None
    }

    pub async fn put(&self, key: String, value: String) -> Result<(), ()> {
        let kv_server = self.find_leader().unwrap();
        let result = kv_server
            .put_append(PutAppendArgs {
                key,
                value,
                op: PutAppendEnum::Put,
                unique_id: UniqueId {
                    clerk_id: 1,
                    sequence_id: self.seq.fetch_add(1, Ordering::Relaxed)
                        as u64,
                },
            })
            .await;
        result.result.map_err(|_| ())
    }

    pub fn spawn_put(
        self: &Arc<Self>,
        key: String,
        value: String,
    ) -> impl Future<Output = Result<(), ()>> {
        let this = self.clone();
        async move { this.put(key, value).await }
    }

    pub async fn get(&self, key: String) -> Result<String, ()> {
        let kv_server = self.find_leader().unwrap();
        let result = kv_server.get(GetArgs { key }).await;
        result.result.map(|v| v.unwrap_or_default()).map_err(|_| ())
    }

    pub fn spawn_get(
        self: &Arc<Self>,
        key: String,
    ) -> impl Future<Output = Result<String, ()>> {
        let this = self.clone();
        async move { this.get(key).await }
    }
}

pub fn make_config(server_count: usize, max_state: Option<usize>) -> Config {
    let (event_queue, clients) = make_grid_clients(server_count);
    let persister = Arc::new(Persister::new());
    let mut kv_servers = vec![];
    let clients: Vec<Vec<&'static InterceptingRpcClient<UniqueKVOp>>> = clients
        .into_iter()
        .map(|v| {
            v.into_iter()
                .map(|c| {
                    let c = Box::leak(Box::new(c));
                    &*c
                })
                .collect()
        })
        .collect();
    for (index, client_vec) in clients.iter().enumerate() {
        let kv_server = KVServer::new(
            client_vec.to_vec(),
            index,
            persister.clone(),
            max_state,
        );
        kv_servers.push(kv_server);
    }

    for clients in clients.iter() {
        for j in 0..server_count {
            clients[j].set_raft(kv_servers[j].raft().clone());
        }
    }

    Config {
        event_queue,
        kv_servers,
        seq: AtomicUsize::new(0),
    }
}
