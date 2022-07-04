use ruaft::utils::integration_test::{
    unpack_append_entries_args, unpack_append_entries_reply,
};
use std::sync::Arc;
use std::time::{Duration, Instant};
use test_configs::interceptor::{make_config, RaftRpcEvent};
use test_utils::init_test_log;

#[test]
fn smoke_test() {
    init_test_log!();
    let server_count = 3;
    let config = make_config(server_count, None);
    let config = Arc::new(config);
    let thread_pool = tokio::runtime::Runtime::new().unwrap();
    let put = thread_pool.spawn(
        config.spawn_put("commit".to_string(), "consistency".to_string()),
    );

    let mut responded = false;
    while let Ok((event, handle)) = config.event_queue.receiver.recv() {
        if let RaftRpcEvent::AppendEntriesResponse(args, reply) = event {
            if let Some(index_term) = unpack_append_entries_args(args) {
                let (term, success) = unpack_append_entries_reply(reply);
                if term == index_term.term && success && index_term.index >= 1 {
                    responded = true;
                    break;
                }
            }
        }
        handle.unblock();
    }
    assert!(responded, "At least one peer must have responded OK");
    let result = thread_pool.block_on(put).unwrap();
    assert!(result.is_ok());

    let get = thread_pool.spawn(config.spawn_get("commit".to_string()));
    let start = Instant::now();
    while let Ok((_event, handle)) = config
        .event_queue
        .receiver
        .recv_timeout(Duration::from_secs(1))
    {
        if get.is_finished() {
            break;
        }
        if start.elapsed() >= Duration::from_secs(1) {
            break;
        }
        handle.unblock();
    }
    assert!(get.is_finished());
    let value = thread_pool.block_on(get).unwrap().unwrap();
    assert_eq!("consistency", value);
}
