use std::sync::Arc;

use scopeguard::defer;

use test_configs::kvraft::config::make_config;
use test_configs::kvraft::generic_test::{generic_test, GenericTestParams};
use test_configs::utils::sleep_election_timeouts;
use test_utils::init_test_log;

#[test]
fn install_snapshot_rpc() {
    init_test_log!();
    const SERVERS: usize = 3;
    const MAX_RAFT_STATE: usize = 1000;
    const KEY: &str = "a";
    let cfg = Arc::new(make_config(SERVERS, false, MAX_RAFT_STATE));
    defer!(cfg.clean_up());

    let mut clerk = cfg.make_clerk();

    cfg.begin("Test: InstallSnapshot RPC (3B)");

    clerk.put("a", "A");
    assert_eq!(clerk.get(KEY), Some("A".to_owned()));
    let (majority, minority) = cfg.partition();
    {
        let mut clerk = cfg.make_limited_clerk(&majority);
        for i in 0..50 {
            let i_str = i.to_string();
            clerk.put(&i_str, &i_str);
        }
        sleep_election_timeouts(1);
        clerk.put("b", "B");
    }

    cfg.check_log_size(MAX_RAFT_STATE * 2)
        .expect("Log does not seem to be trimmed:");

    // Swap majority and minority.
    let (mut majority, mut minority) = (minority, majority);
    majority.push(
        minority
            .pop()
            .expect("There should be at least one server in the majority."),
    );
    cfg.network_partition(&majority, &minority);

    {
        let mut clerk = cfg.make_limited_clerk(&majority);
        clerk.put("c", "C");
        clerk.put("d", "D");
        assert_eq!(clerk.get(KEY), Some("A".to_owned()));
        assert_eq!(clerk.get("b"), Some("B".to_owned()));
        assert_eq!(clerk.get("c"), Some("C".to_owned()));
        assert_eq!(clerk.get("d"), Some("D".to_owned()));
        assert_eq!(clerk.get("1"), Some("1".to_owned()));
        assert_eq!(clerk.get("49"), Some("49".to_owned()));
    }

    cfg.connect_all();
    clerk.put("e", "E");
    assert_eq!(clerk.get("c"), Some("C".to_owned()));
    assert_eq!(clerk.get("e"), Some("E".to_owned()));
    assert_eq!(clerk.get("1"), Some("1".to_owned()));
    assert_eq!(clerk.get("49"), Some("49".to_owned()));

    cfg.end();
}

#[test]
fn snapshot_size() {
    init_test_log!();
    const SERVERS: usize = 3;
    const MAX_RAFT_STATE: usize = 1000;
    const MAX_SNAPSHOT_STATE: usize = 500;
    let cfg = Arc::new(make_config(SERVERS, false, MAX_RAFT_STATE));
    defer!(cfg.clean_up());

    let mut clerk = cfg.make_clerk();

    cfg.begin("Test: snapshot size is reasonable (3B)");

    for _ in 0..200 {
        clerk.put("x", "0");
        assert_eq!(clerk.get("x"), Some("0".to_owned()));
        clerk.put("x", "1");
        assert_eq!(clerk.get("x"), Some("1".to_owned()));
    }

    cfg.check_log_size(MAX_RAFT_STATE * 2)
        .expect("Log does not seem to be trimmed:");
    cfg.check_snapshot_size(MAX_SNAPSHOT_STATE)
        .expect("Snapshot size is too big:");

    cfg.end();
}

#[test]
fn snapshot_recover_test() {
    init_test_log!();
    generic_test(GenericTestParams {
        clients: 1,
        crash: true,
        maxraftstate: Some(1000),
        ..Default::default()
    })
}

#[test]
fn snapshot_recover_many_clients() {
    init_test_log!();
    generic_test(GenericTestParams {
        clients: 20,
        crash: true,
        maxraftstate: Some(1000),
        min_ops: Some(0),
        ..Default::default()
    })
}

#[test]
fn snapshot_unreliable_test() {
    init_test_log!();
    generic_test(GenericTestParams {
        clients: 5,
        unreliable: true,
        maxraftstate: Some(1000),
        ..Default::default()
    })
}

#[test]
fn snapshot_unreliable_recover_test() {
    init_test_log!();
    generic_test(GenericTestParams {
        clients: 5,
        unreliable: true,
        crash: true,
        maxraftstate: Some(1000),
        ..Default::default()
    })
}

#[test]
fn snapshot_unreliable_recover_partition() {
    init_test_log!();
    generic_test(GenericTestParams {
        clients: 5,
        unreliable: true,
        crash: true,
        partition: true,
        maxraftstate: Some(1000),
        min_ops: Some(0),
        ..Default::default()
    })
}
#[test]
fn linearizability() {
    init_test_log!();
    generic_test(GenericTestParams {
        clients: 15,
        unreliable: true,
        partition: true,
        crash: true,
        maxraftstate: Some(1000),
        min_ops: Some(0),
        test_linearizability: true,
    });
}
