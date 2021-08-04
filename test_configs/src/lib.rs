pub mod config;
mod rpcs;

pub use rpcs::{make_rpc_handler, register_server, RpcClient};
