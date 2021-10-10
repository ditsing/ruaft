use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use lazy_static::lazy_static;
use serde_derive::{Deserialize, Serialize};
use warp::Filter;

use crate::one_clerk::create_clerk;
use crate::run::run_kv_instance;

mod kv_service;
mod one_clerk;
mod persister;
mod raft_service;
mod run;
mod utils;

#[derive(Deserialize, Serialize)]
struct PutAppendBody {
    key: String,
    value: String,
}

lazy_static! {
    static ref KV_ADDRS: Vec<SocketAddr> = vec![
        ([127, 0, 0, 1], 9986).into(),
        ([127, 0, 0, 1], 9987).into(),
        ([127, 0, 0, 1], 9988).into(),
    ];
    static ref RAFT_ADDRS: Vec<SocketAddr> = vec![
        ([127, 0, 0, 1], 10006).into(),
        ([127, 0, 0, 1], 10007).into(),
        ([127, 0, 0, 1], 10008).into(),
    ];
    static ref WEB_ADDRS: Vec<SocketAddr> = vec![
        ([0, 0, 0, 0], 9006).into(),
        ([0, 0, 0, 0], 9007).into(),
        ([0, 0, 0, 0], 9008).into(),
    ];
}

const NOT_READY: &str = "Clerk is not ready";

async fn run_web_server(me: usize) {
    let kv_server = run_kv_instance(KV_ADDRS[me], RAFT_ADDRS.clone(), me)
        .await
        .expect("Running kv instance should not fail");

    let is_leader = warp::get()
        .and(warp::path!("kvstore" / "is_leader"))
        .map(move || format!("{:?}", kv_server.raft().get_state()));

    let counter = Arc::new(AtomicUsize::new(0));
    let counter_2 = counter.clone();
    let counter_3 = counter.clone();
    let clerk = create_clerk(KV_ADDRS.clone());

    let get_clerk = clerk.clone();
    let get = warp::get()
        .and(warp::path!("kvstore" / "get" / String))
        .map(move |key: String| {
            let counter = counter.fetch_add(1, Ordering::SeqCst).to_string();
            match get_clerk.get(key.clone()) {
                Some(value) => {
                    key + "!" + counter.as_str() + "!" + value.as_str()
                }
                None => NOT_READY.to_string(),
            }
        });

    let put_clerk = clerk.clone();
    let put = warp::post()
        .and(warp::path!("kvstore" / "put"))
        .and(warp::body::json())
        .map(move |body: PutAppendBody| {
            counter_2.fetch_add(1, Ordering::SeqCst);
            put_clerk.put(body.key, body.value);
            warp::reply::reply()
        });
    let append_clerk = clerk.clone();
    let append = warp::post()
        .and(warp::path!("kvstore" / "append"))
        .and(warp::body::json())
        .map(move |body: PutAppendBody| {
            counter_3.fetch_add(1, Ordering::SeqCst);
            append_clerk.append(body.key, body.value);
            warp::reply::reply()
        });

    let routes = is_leader.or(get).or(put).or(append);
    warp::serve(routes).run(WEB_ADDRS[me]).await;
}

fn main() {
    let me: usize = std::env::args()
        .skip(1)
        .next()
        .unwrap_or_default()
        .parse()
        .expect("An index of the current instance must be passed in");
    test_utils::init_log(format!("durio-instance-{}", me).as_str())
        .expect("Initiating log should not fail");
    let local_logger = test_utils::thread_local_logger::get();

    let thread_pool = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name("durio")
        .on_thread_start(move || {
            test_utils::thread_local_logger::set(local_logger.clone())
        })
        .build()
        .expect("Creating thread pool should not fail");

    thread_pool.block_on(run_web_server(me));
}
