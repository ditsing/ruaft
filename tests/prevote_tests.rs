use scopeguard::defer;

use test_configs::make_config;
use test_configs::raft::config;
use test_configs::utils::sleep_election_timeouts;

#[test]
fn no_disruptive_rejoin() -> config::Result<()> {
    const SERVERS: usize = 5;
    let cfg = make_config!(SERVERS, false);
    defer!(cfg.cleanup());
    cfg.one(100, SERVERS, false)?;
    let initial_term =
        cfg.check_terms()?.expect("Servers should agree on term");

    //  0 <--> 1 <--> 3
    //  ^      ^
    //  |      |
    //  |      v
    //  -----> 2 <--> 4

    cfg.disconnect(4);
    cfg.disconnect(3);

    // The disconnected servers (3 & 4) should not disrupt the exist leader.
    cfg.connect_pair(2, 4);
    cfg.connect_pair(1, 3);

    let leader = cfg.check_one_leader()?;
    cfg.one(200, 3, false)?;

    let (first_term, _) = cfg.leader_start(leader, 300).unwrap();
    assert!(first_term >= initial_term);
    assert!(first_term <= initial_term + 1);

    sleep_election_timeouts(2);

    let (second_term, _) = cfg.leader_start(leader, 400).unwrap();
    assert_eq!(first_term, second_term);

    let index = cfg.one(500, 3, false)?;
    assert_eq!(index, 5);

    Ok(())
}
