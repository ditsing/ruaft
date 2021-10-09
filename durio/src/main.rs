use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use lazy_static::lazy_static;
use serde_derive::{Deserialize, Serialize};
use warp::Filter;

use crate::run::run_kv_instance;

mod kv_service;
mod persister;
mod raft_service;
mod run;
mod utils;

#[derive(Deserialize, Serialize)]
struct PutAppendBody {
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

async fn run_web_server(me: usize) {
    let kv_server = run_kv_instance(KV_ADDRS[me], RAFT_ADDRS.clone(), me)
        .await
        .expect("Running kv instance should not fail");
    let is_leader_kv_server = kv_server.clone();

    let try_get = warp::path::param().and_then(move |_: u32| {
        let kv_server = kv_server.clone();
        async move {
            let value = kv_server
                .get(kvraft::GetArgs {
                    key: "".to_string(),
                    op: kvraft::GetEnum::AllowDuplicate,
                    unique_id: Default::default(),
                })
                .await
                .result
                .expect("Get should not fail");
            let result: Result<String, Infallible> =
                Ok(value.unwrap_or_default());
            result
        }
    });
    let is_leader = warp::path!("kvstore" / "is_leader")
        .map(move || format!("{:?}", is_leader_kv_server.raft().get_state()));

    let counter = Arc::new(AtomicUsize::new(0));
    let counter_clone = counter.clone();
    let get = warp::path!("kvstore" / "get" / String).map(move |key| {
        key + "!" + counter.fetch_add(1, Ordering::SeqCst).to_string().as_str()
    });
    let put = warp::post()
        .and(warp::path!("kvstore" / "put" / String))
        .and(warp::body::json())
        .map(move |key, _body: PutAppendBody| {
            counter_clone.fetch_add(1, Ordering::SeqCst);
            warp::reply::json(&key)
        });
    let append = warp::post()
        .and(warp::path!("kvstore" / "append" / String))
        .and(warp::body::json())
        .map(|key, _body: PutAppendBody| warp::reply::json(&key));

    let routes =
        warp::get().and(get.or(put).or(append).or(is_leader).or(try_get));
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
