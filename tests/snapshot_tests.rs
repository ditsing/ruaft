extern crate kvraft;
#[macro_use]
extern crate scopeguard;

use kvraft::testing_utils::config::{make_config, sleep_election_timeouts};
use std::sync::Arc;

#[test]
fn install_snapshot_rpc() {
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

    cfg.check_log_size(MAX_RAFT_STATE * 2).expect("Hi");

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
