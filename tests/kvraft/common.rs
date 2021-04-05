use labrpc::{Client, RequestMessage};
use rand::{thread_rng, RngCore};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(
    Clone, Debug, Ord, PartialOrd, Eq, PartialEq, Serialize, Deserialize,
)]
pub struct UniqueId {
    clerk_id: u64,
    sequence_id: u64,
}

#[derive(Debug)]
pub struct UniqueIdSequence {
    clerk_id: u64,
    sequence_id: AtomicU64,
}

impl UniqueIdSequence {
    pub fn new() -> Self {
        Self {
            clerk_id: thread_rng().next_u64(),
            sequence_id: AtomicU64::new(0),
        }
    }

    pub fn zero(&self) -> UniqueId {
        UniqueId {
            clerk_id: self.clerk_id,
            sequence_id: 0,
        }
    }

    pub fn inc(&mut self) -> UniqueId {
        let seq = self.sequence_id.fetch_add(1, Ordering::Relaxed);
        UniqueId {
            clerk_id: self.clerk_id,
            sequence_id: seq,
        }
    }
}

pub(crate) const GET: &str = "KVServer.Get";
pub(crate) const PUT_APPEND: &str = "KVServer.PutAppend";

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum PutAppendEnum {
    Put,
    Append,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PutAppendArgs {
    pub key: String,
    pub value: String,
    pub op: PutAppendEnum,

    pub unique_id: UniqueId,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum KvError {
    NoKey,
    Other(String),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PutAppendReply {
    pub result: Result<(), KvError>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GetArgs {
    pub key: String,

    pub unique_id: UniqueId,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GetReply {
    pub result: Result<String, KvError>,
    pub is_retry: bool,
}

pub async fn call_rpc<M: AsRef<str>, A: Serialize, R: DeserializeOwned>(
    client: &Client,
    method: M,
    args: A,
) -> labrpc::Result<R> {
    let data = RequestMessage::from(
        bincode::serialize(&args)
            .expect("Serialization of requests should not fail"),
    );

    let reply = client.call_rpc(method.as_ref().to_owned(), data).await?;

    Ok(bincode::deserialize(reply.as_ref())
        .expect("Deserialization of reply should not fail"))
}
