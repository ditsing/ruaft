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

async fn run_web_server(
    socket_addr: SocketAddr,
    kv_server: Arc<KVServer>,
    kv_addrs: Vec<SocketAddr>,
) {
    let app = axum::Router::new();
    let app = app.route(
        "/kvstore/is_leader",
        axum::routing::get(move || {
            let kv_server = kv_server.clone();
            async move {
                let ret: Result<String, Infallible> =
                    Ok(format!("{:?}", kv_server.raft().get_state()));
                ret
            }
        }),
    );

    let clerk = Arc::new(create_async_clerk(kv_addrs).await);
    let counter = Arc::new(AtomicUsize::new(0));

    let get_clerk = clerk.clone();
    let app = app.route(
        "/kvstore/get/:key",
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
        "/kvstore/put",
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
        "/kvstore/append",
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
    env_logger::init();

    // Run RPC servers in a thread pool. This pool
    // 1. Accepts incoming RPC connections for KV and Raft servers.
    // 2. Sends out RPCs to other Raft instances.
    // Timers are used by RPC handling code in the KV server.
    let rpc_server_thread_pool = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name("durio-rpc")
        .build()
        .expect("Creating thread pool should not fail");

    let kv_server = rpc_server_thread_pool
        .block_on(run_kv_instance(KV_ADDRS[me], RAFT_ADDRS.clone(), me))
        .expect("Running kv instance should not fail");

    // Run web servers in a thread pool. This pool
    // 1. Accepts incoming HTTP connections.
    // 2. Sends out RPCs to KV instances, both local and remote.
    let thread_pool = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name("durio-web")
        .build()
        .expect("Creating thread pool should not fail");

    thread_pool.block_on(run_web_server(
        WEB_ADDRS[me],
        kv_server,
        KV_ADDRS.clone(),
    ));
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    // This test is ignored by default, because it cannot be run with other
    // Ruaft tests at the same time. All other ruaft tests are compiled with
    // feature 'integration-test', which is in conflict with this test. This
    // test intends to verify that durio can be run under normal production
    // setup, i.e. without 'integration-test'.
    #[tokio::test]
    async fn smoke_test() {
        let kv_addrs: Vec<SocketAddr> = vec![
            ([0, 0, 0, 0], 9986).into(),
            ([0, 0, 0, 0], 9987).into(),
            ([0, 0, 0, 0], 9988).into(),
        ];
        let raft_addrs: Vec<SocketAddr> = vec![
            ([0, 0, 0, 0], 10006).into(),
            ([0, 0, 0, 0], 10007).into(),
            ([0, 0, 0, 0], 10008).into(),
        ];
        let web_addrs: Vec<SocketAddr> = vec![
            ([0, 0, 0, 0], 9006).into(),
            ([0, 0, 0, 0], 9007).into(),
            ([0, 0, 0, 0], 9008).into(),
        ];
        env_logger::init();

        // KV servers must be created before the web frontend can be run.
        let mut kv_servers = vec![];
        for (me, kv_addr) in kv_addrs.iter().enumerate() {
            let kv_server = run_kv_instance(*kv_addr, raft_addrs.clone(), me)
                .await
                .expect("Running kv instance should not fail");
            kv_servers.push(kv_server);
        }

        // All servers at `kv_addrs` must be up.
        for (me, kv_server) in kv_servers.into_iter().enumerate() {
            tokio::spawn(run_web_server(
                web_addrs[me],
                kv_server,
                kv_addrs.clone(),
            ));
        }
        tokio::time::sleep(Duration::from_millis(500)).await;

        let mut leader_count = 0;
        for web_addr in web_addrs.iter() {
            let url = format!(
                "http://localhost:{}/kvstore/is_leader",
                web_addr.port()
            );
            let is_leader = reqwest::get(url)
                .await
                .expect("HTTP request should not fail")
                .text()
                .await
                .expect("Results should be string");
            println!("is_leader: {}", is_leader);
            if is_leader.contains("true") {
                leader_count += 1;
            }
        }
        assert_eq!(1, leader_count);

        let body = PutAppendBody {
            key: "hello".to_owned(),
            value: "world".to_owned(),
        };

        for web_addr in web_addrs.iter() {
            let client = reqwest::Client::new();
            client
                .post(format!(
                    "http://localhost:{}/kvstore/put",
                    web_addr.port()
                ))
                .json(&body)
                .send()
                .await
                .expect("HTTP request should not fail");
        }

        for web_addr in web_addrs.iter() {
            let url = format!(
                "http://localhost:{}/kvstore/get/{}",
                web_addr.port(),
                "hello"
            );
            let result = reqwest::get(url)
                .await
                .expect("HTTP request should not fail")
                .text()
                .await
                .expect("hi");
            assert_eq!("hello!0!world", result);
        }
    }
}
