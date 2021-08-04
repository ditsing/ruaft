#![allow(clippy::identity_op)]
use rand::{thread_rng, Rng};
use scopeguard::defer;
use test_configs::{make_config, raft::config};

#[test]
fn basic_agreement() -> config::Result<()> {
    const SERVERS: usize = 5;
    let cfg = make_config!(SERVERS, false);
    defer!(cfg.cleanup());

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

    cfg.end();
    Ok(())
}

#[test]
fn fail_agree() -> config::Result<()> {
    const SERVERS: usize = 3;
    let cfg = make_config!(SERVERS, false);
    defer!(cfg.cleanup());

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
    let cfg = make_config!(SERVERS, false);
    defer!(cfg.cleanup());

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

#[test]
fn rejoin() -> config::Result<()> {
    const SERVERS: usize = 3;
    let cfg = make_config!(SERVERS, false);
    defer!(cfg.cleanup());

    cfg.begin("Test (2B): rejoin of partitioned leader");

    cfg.one(101, SERVERS, true)?;

    // leader network failure
    let leader1 = cfg.check_one_leader()?;
    cfg.disconnect(leader1);

    // make old leader try to agree on some entries
    cfg.leader_start(leader1, 102);
    cfg.leader_start(leader1, 103);
    cfg.leader_start(leader1, 104);

    // new leader commits, also for index=2
    cfg.one(103, 2, true)?;

    // new leader network failure
    let leader2 = cfg.check_one_leader()?;
    cfg.disconnect(leader2);

    // old leader connected again
    cfg.connect(leader1);

    cfg.one(104, 2, true)?;

    // all together now
    cfg.connect(leader2);

    cfg.one(105, SERVERS, true)?;

    cfg.end();
    Ok(())
}

#[test]
fn backup() -> config::Result<()> {
    const SERVERS: usize = 5;
    let cfg = make_config!(SERVERS, false);
    defer!(cfg.cleanup());

    cfg.begin(
        "Test (2B): leader backs up quickly over incorrect follower logs",
    );

    cfg.one(thread_rng().gen(), SERVERS, true)?;

    // put leader and one follower in a partition
    let leader1 = cfg.check_one_leader()?;
    cfg.disconnect((leader1 + 2) % SERVERS);
    cfg.disconnect((leader1 + 3) % SERVERS);
    cfg.disconnect((leader1 + 4) % SERVERS);

    // submit lots of commands that won't commit
    for _ in 0..SERVERS {
        cfg.leader_start(leader1, thread_rng().gen());
    }

    config::sleep_election_timeouts(2);

    cfg.disconnect((leader1 + 0) % SERVERS);
    cfg.disconnect((leader1 + 1) % SERVERS);

    // allow other partition to recover
    cfg.connect((leader1 + 2) % SERVERS);
    cfg.connect((leader1 + 3) % SERVERS);
    cfg.connect((leader1 + 4) % SERVERS);

    // lots of successful commands to new group.
    for _ in 0..50 {
        cfg.one(thread_rng().gen(), 3, true)?;
    }

    // now another partitioned leader and one follower
    let leader2 = cfg.check_one_leader()?;
    let mut other = (leader1 + 2) % SERVERS;
    if leader2 == other {
        other = (leader2 + 1) % SERVERS;
    }
    cfg.disconnect(other);

    // lots more commands that won't commit
    for _ in 0..50 {
        cfg.leader_start(leader2, thread_rng().gen());
    }

    config::sleep_election_timeouts(2);

    // bring original leader back to life,
    for i in 0..SERVERS {
        cfg.disconnect(i);
    }

    cfg.connect((leader1 + 0) % SERVERS);
    cfg.connect((leader1 + 1) % SERVERS);
    cfg.connect(other);

    // lots of successful commands to new group.
    for _ in 0..50 {
        cfg.one(thread_rng().gen(), 3, true)?;
    }

    // now everyone
    for i in 0..SERVERS {
        cfg.connect(i);
    }
    cfg.one(thread_rng().gen(), SERVERS, true)?;

    cfg.end();
    Ok(())
}

#[test]
fn count() -> config::Result<()> {
    const SERVERS: usize = 3;
    let cfg = make_config!(SERVERS, false);
    defer!(cfg.cleanup());

    cfg.begin("Test (2B): RPC counts aren't too high");

    cfg.check_one_leader()?;
    let total = cfg.total_rpcs();
    assert!(
        (1..=30).contains(&total),
        "too many or few RPCs ({}) to elect initial leader",
        total
    );

    let mut retries = 0;
    let (success, total) = loop {
        if retries == 5 {
            break (false, 0);
        }
        if retries != 0 {
            config::sleep_millis(3000);
        }
        retries += 1;

        let leader = cfg.check_one_leader()?;
        let start_total = cfg.total_rpcs();

        const ITERS: usize = 10;
        let (term, start_index) = match cfg.leader_start(leader, 1) {
            Some(pair) => pair,
            None => continue,
        };

        let mut cmds = vec![];
        for i in 1..(ITERS + 2) {
            let cmd: i32 = thread_rng().gen();
            cmds.push(cmd);

            let index = match cfg.leader_start(leader, cmd) {
                Some((new_term, index)) => {
                    if new_term == term {
                        Some(index)
                    } else {
                        None
                    }
                }
                None => None,
            };
            if let Some(index) = index {
                assert_eq!(start_index + i, index, "start() failed");
            } else {
                retries = 0;
                break;
            }
        }
        if retries == 0 {
            continue;
        }
        for i in 1..(ITERS + 1) {
            let cmd = cfg.wait(start_index + i, SERVERS, Some(term))?;
            if let Some(cmd) = cmd {
                assert_eq!(
                    cmd,
                    cmds[i - 1],
                    "wrong value {} committed for index {}; expected {:?}",
                    cmd,
                    start_index + i,
                    cmds
                )
            } else {
                retries = 0;
                break;
            }
        }
        if term != cfg.check_terms()?.expect("terms should be agreed on") {
            retries = 0;
        }
        if retries == 0 {
            continue;
        }

        let diff = cfg.total_rpcs() - start_total;
        if diff > (ITERS + 1 + 3) * 3 {
            panic!("too many RPCs ({}) for {} entries", diff, ITERS);
        }

        break (true, cfg.total_rpcs());
    };

    assert!(success, "term change too often");

    config::sleep_election_timeouts(1);

    let diff = cfg.total_rpcs() - total;
    assert!(
        diff < 3 * 20,
        "too many RPCs ({}) for 1 second of idleness",
        diff
    );

    cfg.end();
    Ok(())
}
