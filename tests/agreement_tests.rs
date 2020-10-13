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
