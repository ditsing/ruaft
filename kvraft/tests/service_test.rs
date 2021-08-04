use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use scopeguard::defer;
use test_configs::kvraft::config::{
    make_config, sleep_election_timeouts, sleep_millis,
};
use test_configs::kvraft::generic_test::{
    generic_test, spawn_clients, GenericTestParams,
};
use test_utils::init_test_log;
use test_utils::thread_local_logger::LocalLogger;

type Result = std::result::Result<(), String>;

fn check_concurrent_results(
    value: String,
    clients: usize,
    expected: Vec<usize>,
) -> Result {
    if !value.starts_with('(') || !value.ends_with(')') {
        return Err(format!("Malformed value string {}", value));
    }
    let inner_value = &value[1..value.len() - 1];
    let mut progress = vec![0; clients];
    for pair_str in inner_value.split(")(") {
        let mut nums = vec![];
        for num_str in pair_str.split(", ") {
            let num: usize = num_str.parse().map_err(|_e| {
                format!("Parsing '{:?}' failed within '{:?}'", num_str, value)
            })?;
            nums.push(num);
        }
        if nums.len() != 2 {
            return Err(format!(
                concat!(
                    "More than two numbers in the same group when",
                    " parsing '{:?}' failed within '{:?}'",
                ),
                pair_str, value,
            ));
        }
        let (client, curr) = (nums[0], nums[1]);
        if progress[client] != curr {
            return Err(format!(
                "Client {} failed, expecting {}, got {}, others are {:?} in {}",
                client, progress[client], curr, progress, value,
            ));
        }
        progress[client] = curr + 1;
    }
    assert_eq!(progress, expected, "Expecting progress in {}", value);
    Ok(())
}

#[test]
fn basic_service() {
    init_test_log!();
    generic_test(GenericTestParams {
        clients: 1,
        ..Default::default()
    });
}

#[test]
fn concurrent_client() {
    init_test_log!();
    generic_test(GenericTestParams {
        clients: 5,
        ..Default::default()
    });
}

#[test]
fn unreliable_many_clients() {
    init_test_log!();
    generic_test(GenericTestParams {
        clients: 5,
        unreliable: true,
        ..Default::default()
    });
}

#[test]
fn unreliable_one_key_many_clients() -> Result {
    init_test_log!();
    const SERVERS: usize = 5;
    let cfg = Arc::new(make_config(SERVERS, true, 0));
    defer!(cfg.clean_up());

    let mut clerk = cfg.make_clerk();

    cfg.begin("Test: concurrent append to same key, unreliable (3A)");

    clerk.put("k", "");

    const CLIENTS: usize = 5;
    const ATTEMPTS: usize = 10;
    let logger = LocalLogger::inherit();
    let client_results =
        spawn_clients(cfg.clone(), CLIENTS, move |index, mut clerk| {
            logger.clone().attach();
            for i in 0..ATTEMPTS {
                clerk.append("k", format!("({}, {})", index, i));
            }
        });
    for client_result in client_results {
        client_result.join().expect("Client should never fail");
    }

    let value = clerk.get("k").expect("Key should exist");

    check_concurrent_results(value, CLIENTS, vec![ATTEMPTS; CLIENTS])
}

#[test]
fn one_partition() -> Result {
    init_test_log!();
    const SERVERS: usize = 5;
    let cfg = Arc::new(make_config(SERVERS, false, 0));
    defer!(cfg.clean_up());

    cfg.begin("Test: progress in majority (3A)");

    const KEY: &str = "1";
    let mut clerk = cfg.make_clerk();
    clerk.put(KEY, "13");

    let (majority, minority) = cfg.partition();

    assert!(minority.len() < majority.len());
    assert_eq!(minority.len() + majority.len(), SERVERS);

    let mut clerk_majority = cfg.make_limited_clerk(&majority);
    let mut clerk_minority1 = cfg.make_limited_clerk(&minority);
    let mut clerk_minority2 = cfg.make_limited_clerk(&minority);

    clerk_majority.put(KEY, "14");
    assert_eq!(clerk_majority.get(KEY), Some("14".to_owned()));

    cfg.begin("Test: no progress in minority (3A)");
    let counter = Arc::new(AtomicUsize::new(0));
    let counter1 = counter.clone();
    std::thread::spawn(move || {
        clerk_minority1.put(KEY, "15");
        counter1.fetch_or(1, Ordering::SeqCst);
    });
    let counter2 = counter.clone();
    std::thread::spawn(move || {
        clerk_minority2.get(KEY);
        counter2.fetch_or(2, Ordering::SeqCst);
    });

    sleep_millis(1000);

    assert_eq!(counter.load(Ordering::SeqCst), 0);

    assert_eq!(clerk_majority.get(KEY), Some("14".to_owned()));
    clerk_majority.put(KEY, "16");
    assert_eq!(clerk_majority.get(KEY), Some("16".to_owned()));

    cfg.begin("Test: completion after heal (3A)");

    cfg.connect_all();
    cfg.connect_all_clerks();

    sleep_election_timeouts(1);
    for _ in 0..100 {
        sleep_millis(60);
        if counter.load(Ordering::SeqCst) == 3 {
            break;
        }
    }

    assert_eq!(counter.load(Ordering::SeqCst), 3);
    assert_eq!(clerk.get(KEY), Some("15".to_owned()));

    Ok(())
}

#[test]
fn many_partitions_one_client() {
    init_test_log!();
    generic_test(GenericTestParams {
        clients: 1,
        partition: true,
        ..Default::default()
    });
}

#[test]
fn many_partitions_many_client() {
    init_test_log!();
    generic_test(GenericTestParams {
        clients: 5,
        partition: true,
        ..Default::default()
    });
}

#[test]
fn persist_one_client() {
    init_test_log!();
    generic_test(GenericTestParams {
        clients: 1,
        crash: true,
        ..Default::default()
    });
}

#[test]
fn persist_concurrent() {
    init_test_log!();
    generic_test(GenericTestParams {
        clients: 5,
        crash: true,
        ..Default::default()
    });
}

#[test]
fn persist_concurrent_unreliable() {
    init_test_log!();
    generic_test(GenericTestParams {
        clients: 5,
        unreliable: true,
        crash: true,
        ..Default::default()
    });
}

#[test]
fn persist_partition() {
    init_test_log!();
    generic_test(GenericTestParams {
        clients: 5,
        partition: true,
        crash: true,
        ..Default::default()
    });
}

#[test]
fn persist_partition_unreliable() {
    init_test_log!();
    generic_test(GenericTestParams {
        clients: 5,
        unreliable: true,
        partition: true,
        crash: true,
        min_ops: Some(5),
        ..Default::default()
    });
}

#[test]
fn linearizability() {
    init_test_log!();
    generic_test(GenericTestParams {
        clients: 15,
        unreliable: true,
        partition: true,
        crash: true,
        maxraftstate: None,
        min_ops: Some(0),
        test_linearizability: true,
    });
}
