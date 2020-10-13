extern crate labrpc;
extern crate ruaft;
#[macro_use]
extern crate anyhow;

mod config;

#[test]
fn basic_agreement() -> config::Result<()> {
    const SERVERS: usize = 5;
    let cfg = config::make_config(SERVERS, false);
    let _guard = cfg.deferred_cleanup();

    cfg.begin("Test (2B): basic agreement");
    for index in 1..4 {
        let committed = cfg.committed_count(index)?;
        assert_eq!(0, committed.0, "some have committed before start()");

        let commit_index = cfg.one(index as i32 * 100, SERVERS, false)?;
        assert_eq!(
            index, commit_index,
            "got index {} but expected {}",
            commit_index, index
        );
    }

    Ok(())
}

#[test]
fn fail_agree() -> config::Result<()> {
    const SERVERS: usize = 3;
    let cfg = config::make_config(SERVERS, false);
    let _guard = cfg.deferred_cleanup();

    cfg.begin("Test (2B): agreement despite follower disconnection");

    cfg.one(101, SERVERS, false)?;

    // follower network disconnection
    let leader = cfg.check_one_leader()?;
    cfg.disconnect((leader + 1) % SERVERS);

    // agree despite one disconnected server?
    cfg.one(102, SERVERS - 1, false)?;
    cfg.one(103, SERVERS - 1, false)?;
    config::sleep_election_timeouts(1);
    cfg.one(104, SERVERS - 1, false)?;
    cfg.one(105, SERVERS - 1, false)?;

    // re-connect
    cfg.connect((leader + 1) % SERVERS);

    // agree with full set of servers?
    cfg.one(106, SERVERS, true)?;
    config::sleep_election_timeouts(1);
    cfg.one(107, SERVERS, true)?;

    cfg.end();

    Ok(())
}

#[test]
fn fail_no_agree() -> config::Result<()> {
    const SERVERS: usize = 5;
    let cfg = config::make_config(SERVERS, false);
    let _guard = cfg.deferred_cleanup();

    cfg.begin("Test (2B): no agreement if too many followers disconnect");

    cfg.one(10, SERVERS, false)?;

    // 3 of 5 followers disconnect
    let leader = cfg.check_one_leader()?;
    cfg.disconnect((leader + 1) % SERVERS);
    cfg.disconnect((leader + 2) % SERVERS);
    cfg.disconnect((leader + 3) % SERVERS);

    let result = cfg.leader_start(leader, 20);
    assert!(result.is_some(), "leader rejected start()");
    let index = result.unwrap().1;
    assert_eq!(2, index, "expected index 2, got {}", index);

    config::sleep_election_timeouts(2);

    let (commit_count, _) = cfg.committed_count(index)?;
    assert_eq!(
        0, commit_count,
        "{} committed but no majority",
        commit_count
    );

    // repair
    cfg.connect((leader + 1) % SERVERS);
    cfg.connect((leader + 2) % SERVERS);
    cfg.connect((leader + 3) % SERVERS);

    // the disconnected majority may have chosen a leader from
    // among their own ranks, forgetting index 2.
    let leader2 = cfg.check_one_leader()?;
    let result = cfg.leader_start(leader2, 30);
    assert!(result.is_some(), "leader2 rejected start()");
    let index = result.unwrap().1;
    assert!(index == 2 || index == 3, "unexpected index {}", index);

    cfg.one(1000, SERVERS, true)?;

    cfg.end();

    Ok(())
}
