use std::sync::Arc;
use std::time::{Duration, Instant};

use ruaft::utils::integration_test::{
    unpack_append_entries_args, unpack_append_entries_reply,
};
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

#[test]
fn delayed_commit_consistency_test() {
    init_test_log!();
    let server_count = 3;
    let config = Arc::new(make_config(server_count, None));
    let thread_pool = tokio::runtime::Runtime::new().unwrap();

    let first_write = thread_pool.spawn(
        config.spawn_put("consistency".to_string(), "failed".to_string()),
    );
    let mut write_handle = None;
    while let Ok((event, handle)) = config.event_queue.receiver.recv() {
        if let RaftRpcEvent::AppendEntriesResponse(args, reply) = event {
            if let Some(index_term) = unpack_append_entries_args(args) {
                let (term, success) = unpack_append_entries_reply(reply);
                if term == index_term.term && success && index_term.index >= 1 {
                    if write_handle.is_none() {
                        write_handle.replace(handle);
                        break;
                    } else {
                        handle.reply_interrupted_error();
                    }
                }
            }
        }
    }
    let write_handle = write_handle.unwrap();
    let leader = write_handle.from;
    assert!(
        config.kv_servers[leader].raft().get_state().1,
        "leader should still be leader"
    );

    // Block everything from/to leader until we see a new leader.
    let mut new_leader = leader;
    while let Ok((_event, handle)) = config.event_queue.receiver.recv() {
        let from = handle.from;
        if from == leader || handle.to == leader {
            handle.reply_interrupted_error();
        } else {
            handle.unblock();
            if config.kv_servers[from].raft().get_state().1 {
                new_leader = from;
                break;
            }
        }
    }
    assert_ne!(new_leader, leader, "A new leader must have been elected");
    assert_eq!(1, config.kv_servers[leader].raft().get_state().0 .0);

    let second_write = thread_pool.spawn(config.spawn_put_to_kv(
        new_leader,
        "consistency".to_string(),
        "guaranteed".to_string(),
    ));
    while let Ok((_event, handle)) = config.event_queue.receiver.recv() {
        if handle.from == leader || handle.to == leader {
            handle.reply_interrupted_error();
        } else {
            handle.unblock();
        }
        if second_write.is_finished() {
            break;
        }
    }
    assert_eq!(1, config.kv_servers[leader].raft().get_state().0 .0);
    assert!(second_write.is_finished());
    thread_pool.block_on(second_write).unwrap().unwrap();

    let read = thread_pool
        .spawn(config.spawn_get_from_kv(leader, "consistency".to_string()));
    // Spare kv server some time to handle the request.
    std::thread::sleep(Duration::from_millis(100));

    // Unblocks the write response.
    write_handle.unblock();

    thread_pool.block_on(first_write).unwrap().unwrap();

    if let Ok(result) = thread_pool.block_on(read).unwrap() {
        // This is so wrong. The second write was successful.
        assert_eq!(result, "failed");
    } else {
        panic!("The read request should not timeout");
    }
}
