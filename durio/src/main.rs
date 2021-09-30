use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use serde_derive::{Deserialize, Serialize};
use warp::Filter;

#[derive(Deserialize, Serialize)]
struct PutAppendBody {
    value: String,
}

#[tokio::main]
async fn main() {
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
