#![allow(clippy::identity_op)]

use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use rand::{thread_rng, Rng};
use scopeguard::defer;

use test_configs::utils::{
    sleep_election_timeouts, sleep_millis, LONG_ELECTION_TIMEOUT_MILLIS,
};
use test_configs::{make_config, raft::config};

#[test]
fn persist1() -> config::Result<()> {
    const SERVERS: usize = 5;
    let cfg = make_config!(SERVERS, false);
    defer!(cfg.cleanup());

    cfg.begin("Test (2C): basic persistence");

    cfg.one(11, SERVERS, true)?;

    // crash and re-start all
    for i in 0..SERVERS {
        cfg.start1(i)?;
    }
    for i in 0..SERVERS {
        cfg.disconnect(i);
        cfg.connect(i);
    }

    cfg.one(12, SERVERS, true)?;

    let leader1 = cfg.check_one_leader()?;
    cfg.disconnect(leader1);
    cfg.start1(leader1)?;
    cfg.connect(leader1);

    cfg.one(13, SERVERS, true)?;

    let leader2 = cfg.check_one_leader()?;
    cfg.disconnect(leader2);
    cfg.one(14, SERVERS - 1, true)?;
    cfg.start1(leader2)?;
    cfg.connect(leader2);

    // wait for leader2 to join before killing i3
    cfg.wait(4, SERVERS, None)?;

    let i3 = (cfg.check_one_leader()? + 1) % SERVERS;
    cfg.disconnect(i3);
    cfg.one(15, SERVERS - 1, true)?;
    cfg.start1(i3)?;
    cfg.connect(i3);

    cfg.one(16, SERVERS, true)?;

    cfg.end();
    Ok(())
}

#[test]
fn persist2() -> config::Result<()> {
    const SERVERS: usize = 5;
    let cfg = make_config!(SERVERS, false);
    defer!(cfg.cleanup());

    cfg.begin("Test (2C): more persistence");

    let mut index = 1;
    for _ in 0..5 {
        cfg.one(10 + index, SERVERS, true)?;
        index += 1;

        let leader1 = cfg.check_one_leader()?;

        cfg.disconnect((leader1 + 1) % SERVERS);
        cfg.disconnect((leader1 + 2) % SERVERS);

        cfg.one(10 + index, SERVERS - 2, true)?;

        index += 1;

        cfg.disconnect((leader1 + 0) % SERVERS);
        cfg.disconnect((leader1 + 3) % SERVERS);
        cfg.disconnect((leader1 + 4) % SERVERS);

        cfg.start1((leader1 + 1) % SERVERS)?;
        cfg.start1((leader1 + 2) % SERVERS)?;
        cfg.connect((leader1 + 1) % SERVERS);
        cfg.connect((leader1 + 2) % SERVERS);

        sleep_election_timeouts(1);

        cfg.start1((leader1 + 3) % SERVERS)?;
        cfg.connect((leader1 + 3) % SERVERS);

        cfg.one(10 + index, SERVERS - 2, true)?;

        index += 1;

        cfg.connect((leader1 + 4) % SERVERS);
        cfg.connect((leader1 + 0) % SERVERS);
    }

    cfg.one(1000, SERVERS, true)?;

    cfg.end();
    Ok(())
}

#[test]
fn persist3() -> config::Result<()> {
    const SERVERS: usize = 3;
    let cfg = make_config!(SERVERS, false);
    defer!(cfg.cleanup());

    cfg.begin(
        "Test (2C): partitioned leader and one follower crash, leader restarts",
    );

    cfg.one(101, 3, true)?;

    let leader = cfg.check_one_leader()?;
    cfg.disconnect((leader + 2) % SERVERS);

    cfg.one(102, 2, true)?;

    cfg.crash1((leader + 0) % SERVERS);
    cfg.crash1((leader + 1) % SERVERS);
    cfg.connect((leader + 2) % SERVERS);
    cfg.start1((leader + 0) % SERVERS)?;
    cfg.connect((leader + 0) % SERVERS);

    cfg.one(103, 2, true)?;

    cfg.start1((leader + 1) % SERVERS)?;
    cfg.connect((leader + 1) % SERVERS);

    cfg.one(104, SERVERS, true)?;

    cfg.end();
    Ok(())
}

#[test]
fn figure8() -> config::Result<()> {
    const SERVERS: usize = 5;
    let cfg = make_config!(SERVERS, false);
    defer!(cfg.cleanup());

    cfg.begin("Test (2C): Figure 8");

    cfg.one(thread_rng().gen(), 1, true)?;

    let mut nup = SERVERS;
    for _ in 0..1000 {
        let mut leader = None;
        for i in 0..SERVERS {
            if cfg.is_server_alive(i)
                && cfg.leader_start(i, thread_rng().gen()).is_some()
            {
                leader = Some(i);
            }
        }

        let millis_upper = if thread_rng().gen_ratio(100, 1000) {
            LONG_ELECTION_TIMEOUT_MILLIS >> 1
        } else {
            // Magic number 13?
            13
        };
        let millis = thread_rng().gen_range(0..millis_upper);
        sleep_millis(millis);

        if let Some(leader) = leader {
            cfg.crash1(leader);
            nup -= 1;
        }

        if nup < 3 {
            let index = thread_rng().gen_range(0..SERVERS);
            if !cfg.is_server_alive(index) {
                cfg.start1(index)?;
                cfg.connect(index);
                nup += 1
            }
        }
    }

    for index in 0..SERVERS {
        if !cfg.is_server_alive(index) {
            cfg.start1(index)?;
            cfg.connect(index);
        }
    }

    cfg.one(thread_rng().gen(), SERVERS, true)?;

    cfg.end();
    Ok(())
}

#[test]
fn unreliable_agree() -> config::Result<()> {
    const SERVERS: usize = 5;
    let cfg = Arc::new(make_config!(SERVERS, true));
    defer!(cfg.cleanup());

    cfg.begin("Test (2C): unreliable agreement");

    let mut handles = vec![];
    for iters in 1..50 {
        for j in 0..4 {
            let cfg = cfg.clone();
            let logger = test_utils::thread_local_logger::get();
            let handle = std::thread::spawn(move || {
                test_utils::thread_local_logger::set(logger);
                cfg.one(100 * iters + j, 1, true)
            });
            handles.push(handle);
        }

        cfg.one(iters, 1, true)?;
    }

    cfg.set_unreliable(false);

    for handle in handles {
        handle.join().expect("Thread join should not fail")?;
    }

    cfg.one(100, SERVERS, true)?;

    cfg.end();
    Ok(())
}

#[test]
fn figure8_unreliable() -> config::Result<()> {
    const SERVERS: usize = 5;
    let cfg = make_config!(SERVERS, false);
    defer!(cfg.cleanup());

    cfg.begin("Test (2C): Figure 8 (unreliable)");

    cfg.one(thread_rng().gen_range(0..10000), 1, true)?;

    let mut nup = SERVERS;
    for iters in 0..1000 {
        if iters == 200 {
            cfg.set_long_reordering(true);
        }

        let mut leader = None;
        for i in 0..SERVERS {
            if cfg.is_server_alive(i)
                && cfg.leader_start(i, thread_rng().gen()).is_some()
                && cfg.is_connected(i)
            {
                leader = Some(i);
            }
        }

        let millis_upper = if thread_rng().gen_ratio(100, 1000) {
            LONG_ELECTION_TIMEOUT_MILLIS >> 1
        } else {
            // Magic number 13?
            13
        };
        let millis = thread_rng().gen_range(0..millis_upper);
        sleep_millis(millis);

        if let Some(leader) = leader {
            if thread_rng().gen_ratio(1, 2) {
                cfg.disconnect(leader);
                nup -= 1;
            }
        }

        if nup < 3 {
            let index = thread_rng().gen_range(0..SERVERS);
            if !cfg.is_connected(index) {
                cfg.connect(index);
                nup += 1
            }
        }
    }

    for index in 0..SERVERS {
        if !cfg.is_connected(index) {
            cfg.connect(index);
        }
    }

    cfg.one(thread_rng().gen_range(0..10000), SERVERS, true)?;

    cfg.end();
    Ok(())
}

fn internal_churn(
    unreliable: bool,
    test_name: &'static str,
) -> config::Result<()> {
    const SERVERS: usize = 5;
    let cfg = Arc::new(config::make_config(SERVERS, false, test_name));
    defer!(cfg.cleanup());

    if unreliable {
        cfg.begin("Test (2C): unreliable churn");
    } else {
        cfg.begin("Test (2C): churn");
    }

    let stop = Arc::new(AtomicBool::new(false));
    let mut handles = vec![];
    for client_index in 0..3 {
        let stop = stop.clone();
        let cfg = cfg.clone();
        let logger = test_utils::thread_local_logger::get();
        let handle = std::thread::spawn(move || {
            test_utils::thread_local_logger::set(logger);
            let mut cmds = vec![];
            while !stop.load(Ordering::Acquire) {
                let cmd = thread_rng().gen();
                let mut index = None;
                for i in 0..SERVERS {
                    if cfg.is_server_alive(i) {
                        let start = cfg.leader_start(i, cmd);
                        if start.is_some() {
                            index = Some(i);
                        }
                    }
                }

                if let Some(index) = index {
                    for millis in [10, 20, 50, 100, 200].iter() {
                        let (cmd_index, cmd_committed) =
                            // somehow the compiler cannot infer the error type.
                            match cfg.committed_count(index) {
                                Ok(t) => t,
                                Err(e) => return Err(e),
                            };
                        #[allow(clippy::collapsible_if)]
                        if cmd_index > 0 {
                            if cmd_committed == cmd {
                                cmds.push(cmd);
                            }
                            // The contract we started might not get
                        }
                        sleep_millis(*millis);
                    }
                } else {
                    sleep_millis(79 + client_index * 17);
                }
            }

            Ok(cmds)
        });
        handles.push(handle);
    }

    for _ in 0..20 {
        if thread_rng().gen_ratio(200, 1000) {
            cfg.disconnect(thread_rng().gen_range(0..SERVERS));
        }
        if thread_rng().gen_ratio(500, 1000) {
            let server = thread_rng().gen_range(0..SERVERS);
            if !cfg.is_server_alive(server) {
                cfg.start1(server)?;
            }
            cfg.connect(server);
        }

        if thread_rng().gen_ratio(200, 1000) {
            let server = thread_rng().gen_range(0..SERVERS);
            if cfg.is_server_alive(server) {
                cfg.crash1(server);
            }
        }
        sleep_millis(LONG_ELECTION_TIMEOUT_MILLIS / 10 * 7);
    }

    sleep_election_timeouts(1);
    cfg.set_unreliable(false);
    for i in 0..SERVERS {
        if !cfg.is_server_alive(i) {
            cfg.start1(i)?;
        }
        cfg.connect(i);
    }

    stop.store(true, Ordering::Release);
    let mut all_cmds = vec![];
    for handle in handles {
        let mut cmds = handle.join().expect("Client should not fail")?;
        all_cmds.append(&mut cmds);
    }

    sleep_election_timeouts(1);

    let last_cmd_index = cfg.one(thread_rng().gen(), SERVERS, true)?;
    let mut consented = vec![];
    for cmd_index in 1..last_cmd_index + 1 {
        let cmd = cfg.wait(cmd_index, SERVERS, None)?;
        let cmd = cmd.expect("There should always be a command");
        consented.push(cmd);
    }

    for cmd in all_cmds {
        assert!(
            consented.contains(&cmd),
            "Cmd {} not found in {:?}",
            cmd,
            consented
        );
    }

    cfg.end();
    Ok(())
}

#[test]
fn reliable_churn() -> config::Result<()> {
    internal_churn(false, stdext::function_name!())
}

#[test]
fn unreliable_churn() -> config::Result<()> {
    internal_churn(true, stdext::function_name!())
}
