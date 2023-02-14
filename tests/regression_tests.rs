use std::sync::Arc;
use std::time::{Duration, Instant};

use ruaft::utils::integration_test::{
    unpack_append_entries_args, unpack_append_entries_reply,
};
use test_configs::interceptor::{make_config, RaftRpcEvent};

#[tokio::test(flavor = "multi_thread")]
async fn smoke_test() {
    test_utils::init_log("regression_tests-smoke_test")
        .expect("Initializing test log should never fail");
    let server_count = 3;
    let config = make_config(server_count, None);
    let config = Arc::new(config);
    let put = tokio::spawn(
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
    let result = put.await.unwrap();
    assert!(result.is_ok());

    let get = tokio::spawn(config.spawn_get("commit".to_string()));
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
    let value = get.await.unwrap().unwrap();
    assert_eq!("consistency", value);
}

#[tokio::test(flavor = "multi_thread")]
async fn delayed_commit_consistency_test() {
    test_utils::init_log("regression_tests-delayed_commit_consistency_test")
        .expect("Initializing test log should never fail");
    let server_count = 3;
    let config = Arc::new(make_config(server_count, None));

    let first_write = tokio::spawn(
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

    let second_write = tokio::spawn(config.spawn_put_to_kv(
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
    second_write.await.unwrap().unwrap();

    let read = tokio::spawn(
        config.spawn_get_from_kv(leader, "consistency".to_string()),
    );
    // Spare kv server some time to handle the request.
    std::thread::sleep(Duration::from_millis(100));

    // Unblocks the write response.
    write_handle.unblock();
    first_write.await.unwrap().unwrap();

    assert!(read.await.unwrap().is_err());
}
