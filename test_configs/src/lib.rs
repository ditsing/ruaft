#![allow(clippy::uninlined_format_args)]

mod in_memory_storage;
pub mod interceptor;
pub mod kvraft;
pub mod raft;
mod rpcs;
pub mod utils;

pub use in_memory_storage::InMemoryStorage;
use rpcs::{register_kv_server, register_server, RpcClient};
