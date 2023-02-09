use scopeguard::defer;

use test_configs::make_config;
use test_configs::raft::config;
use test_configs::utils::sleep_election_timeouts;

#[test]
fn no_disruptive_rejoin() -> config::Result<()> {
    const SERVERS: usize = 5;
    let cfg = make_config!(SERVERS, false);
    defer!(cfg.cleanup());

    let initial_leader = cfg.check_one_leader()?;
    cfg.one(100, SERVERS, false)?;
    let initial_term =
        cfg.check_terms()?.expect("Servers should agree on term");

    //  0 <--> 1 <--> 3
    //  ^      ^
    //  |      |
    //  |      v
    //  -----> 2 <--> 4

    let disruptor_1: usize = (initial_leader + 3) % SERVERS;
    let disruptor_2: usize = (initial_leader + 4) % SERVERS;
    cfg.disconnect(disruptor_1);
    cfg.disconnect(disruptor_2);

    // The disconnected servers (3 & 4) should not disrupt the exist leader.
    let disrupted_1: usize = (initial_leader + 1) % SERVERS;
    let disrupted_2: usize = (initial_leader + 2) % SERVERS;
    cfg.connect_pair(disrupted_1, disruptor_1);
    cfg.connect_pair(disrupted_2, disrupted_2);

    let leader = cfg.check_one_leader()?;
    cfg.one(200, 3, false)?;

    let (first_term, _) = cfg.leader_start(leader, 300).unwrap();
    assert_eq!(initial_leader, leader);
    assert_eq!(first_term, initial_term);

    sleep_election_timeouts(2);

    let (second_term, _) = cfg.leader_start(leader, 400).unwrap();
    assert_eq!(first_term, second_term);

    let index = cfg.one(500, 3, false)?;
    assert_eq!(index, 5);

    Ok(())
}

#[test]
fn disruptive_liveness() -> config::Result<()> {
    const SERVERS: usize = 5;
    let cfg = make_config!(SERVERS, false);
    defer!(cfg.cleanup());

    let initial_leader = cfg.check_one_leader()?;
    cfg.one(100, SERVERS, false)?;
    cfg.check_terms()?.expect("Servers should agree on term");

    //  3 <--> 2      4
    //  ^      ^
    //  |      |
    //  |      v
    //  -----> 1 <--> 0

    // Disconnect 4
    let disruptor = (initial_leader + 4) % SERVERS;
    cfg.disconnect(disruptor);

    // Disconnect (0, 2) and (0, 3)
    cfg.disconnect_pair(initial_leader, (initial_leader + 2) % SERVERS);
    cfg.disconnect_pair(initial_leader, (initial_leader + 3) % SERVERS);

    sleep_election_timeouts(1);

    let leader = cfg.check_one_leader()?;
    cfg.one(200, 3, false)?;

    assert_ne!(initial_leader, leader);
    assert_ne!(disruptor, leader);

    Ok(())
}
