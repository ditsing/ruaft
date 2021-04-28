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
    let cfg = Arc::new(make_config(SERVERS, true, MAX_RAFT_STATE));
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
}
