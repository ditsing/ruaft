use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use axum::extract::{Json, Path};
use lazy_static::lazy_static;
use serde_derive::{Deserialize, Serialize};

use kvraft::KVServer;

use crate::kv_service::create_async_clerk;
use crate::run::run_kv_instance;

mod kv_service;
mod persister;
mod raft_service;
mod run;
mod utils;

#[derive(Deserialize, Serialize)]
struct PutAppendBody {
    key: String,
    value: String,
}

const IP_ONE: [u8; 4] = [10, 1, 1, 198];
const IP_TWO: [u8; 4] = [10, 1, 1, 51];
const IP_THREE: [u8; 4] = [10, 1, 1, 56];

lazy_static! {
    static ref KV_ADDRS: Vec<SocketAddr> = vec![
        (IP_ONE, 9986).into(),
        (IP_TWO, 9987).into(),
        (IP_THREE, 9988).into(),
    ];
    static ref RAFT_ADDRS: Vec<SocketAddr> = vec![
        (IP_ONE, 10006).into(),
        (IP_TWO, 10007).into(),
        (IP_THREE, 10008).into(),
    ];
    static ref WEB_ADDRS: Vec<SocketAddr> = vec![
        ([0, 0, 0, 0], 9006).into(),
        ([0, 0, 0, 0], 9007).into(),
        ([0, 0, 0, 0], 9008).into(),
    ];
}

async fn run_web_server(socket_addr: SocketAddr, kv_server: Arc<KVServer>) {
    let app = axum::Router::new();
    let app = app.route(
        "kvstore/is_leader",
        axum::routing::get(move || {
            let kv_server = kv_server.clone();
            async move {
                let ret: Result<String, Infallible> =
                    Ok(format!("{:?}", kv_server.raft().get_state()));
                ret
            }
        }),
    );

    let clerk = Arc::new(create_async_clerk(KV_ADDRS.clone()).await);
    let counter = Arc::new(AtomicUsize::new(0));

    let get_clerk = clerk.clone();
    let app = app.route(
        "kvstore/get/:key",
        axum::routing::get(move |Path(key): Path<String>| {
            let counter = counter.fetch_add(1, Ordering::Relaxed).to_string();
            let get_clerk = get_clerk.clone();
            async move {
                let value = get_clerk.get(&key).await.unwrap_or_default();
                let ret: Result<String, Infallible> =
                    Ok(key + "!" + counter.as_str() + "!" + value.as_str());
                ret
            }
        }),
    );

    let put_clerk = clerk.clone();
    let app = app.route(
        "kvstore/put",
        axum::routing::post(move |Json(body): Json<PutAppendBody>| {
            let put_clerk = put_clerk.clone();
            async move {
                put_clerk.put(body.key, body.value).await;
                let ret: Result<String, Infallible> = Ok("OK".to_string());
                ret
            }
        }),
    );

    let append_clerk = clerk.clone();
    let app = app.route(
        "kvstore/append",
        axum::routing::post(move |Json(body): Json<PutAppendBody>| {
            let append_clerk = append_clerk.clone();
            async move {
                append_clerk.append(body.key, body.value).await;
                let ret: Result<String, Infallible> = Ok("OK".to_string());
                ret
            }
        }),
    );

    axum::Server::bind(&socket_addr)
        .serve(app.into_make_service())
        .await
        .expect("Webserver should not fail")
}

fn main() {
    let me: usize = std::env::args()
        .nth(1)
        .unwrap_or_default()
        .parse()
        .expect("An index of the current instance must be passed in");
    test_utils::init_log(format!("durio-instance-{}", me).as_str())
        .expect("Initiating log should not fail");

    // Run RPC servers in a thread pool. This pool
    // 1. Accepts incoming RPC connections for KV and Raft servers.
    // 2. Sends out RPCs to other Raft instances.
    // Timers are used by RPC handling code in the KV server.
    let local_logger = test_utils::thread_local_logger::get();
    let rpc_server_thread_pool = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name("durio-rpc")
        .on_thread_start(move || {
            test_utils::thread_local_logger::set(local_logger.clone())
        })
        .build()
        .expect("Creating thread pool should not fail");

    let kv_server = rpc_server_thread_pool
        .block_on(run_kv_instance(KV_ADDRS[me], RAFT_ADDRS.clone(), me))
        .expect("Running kv instance should not fail");

    // Run web servers in a thread pool. This pool
    // 1. Accepts incoming HTTP connections.
    // 2. Sends out RPCs to KV instances, both local and remote.
    let local_logger = test_utils::thread_local_logger::get();
    let thread_pool = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name("durio-web")
        .on_thread_start(move || {
            test_utils::thread_local_logger::set(local_logger.clone())
        })
        .build()
        .expect("Creating thread pool should not fail");

    thread_pool.block_on(run_web_server(WEB_ADDRS[me], kv_server));
}
