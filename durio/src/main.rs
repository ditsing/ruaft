use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

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

#[tokio::main]
async fn main() {
    run_kv_instance(([127, 0, 0, 1], 9988).into(), vec![], 0)
        .await
        .expect("Running kv instance should not fail");

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

    let routes = warp::get().and(get.or(put).or(append));
    warp::serve(routes).run(([0, 0, 0, 0], 9090)).await;
}
