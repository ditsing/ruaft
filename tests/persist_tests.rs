#[macro_use]
extern crate anyhow;
extern crate bytes;
extern crate labrpc;
extern crate ruaft;

use rand::{thread_rng, Rng};
use std::sync::Arc;

mod config;

#[test]
fn persist() -> config::Result<()> {
    const SERVERS: usize = 5;
    let cfg = config::make_config(SERVERS, false);
    let _guard = cfg.deferred_cleanup();

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

    drop(_guard);
    Ok(())
}

#[test]
fn persist2() -> config::Result<()> {
    const SERVERS: usize = 5;
    let cfg = config::make_config(SERVERS, false);
    let _guard = cfg.deferred_cleanup();

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

        config::sleep_election_timeouts(1);

        cfg.start1((leader1 + 3) % SERVERS)?;
        cfg.connect((leader1 + 3) % SERVERS);

        cfg.one(10 + index, SERVERS - 2, true)?;

        index += 1;

        cfg.connect((leader1 + 4) % SERVERS);
        cfg.connect((leader1 + 0) % SERVERS);
    }

    cfg.one(1000, SERVERS, true)?;

    cfg.end();

    drop(_guard);
    Ok(())
}

#[test]
fn persist3() -> config::Result<()> {
    const SERVERS: usize = 3;
    let cfg = config::make_config(SERVERS, false);
    let _guard = cfg.deferred_cleanup();

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

    drop(_guard);

    Ok(())
}

#[test]
fn figure8() -> config::Result<()> {
    const SERVERS: usize = 5;
    let cfg = config::make_config(SERVERS, false);
    let _guard = cfg.deferred_cleanup();

    cfg.begin("Test (2C): Figure 8");

    cfg.one(thread_rng().gen(), 1, true)?;

    let mut nup = SERVERS;
    for _ in 0..1000 {
        let mut leader = None;
        for i in 0..SERVERS {
            if cfg.is_server_alive(i) {
                if let Some(_) = cfg.leader_start(i, thread_rng().gen()) {
                    leader = Some(i);
                }
            }
        }

        let millis_upper = if thread_rng().gen_ratio(100, 1000) {
            config::LONG_ELECTION_TIMEOUT_MILLIS >> 1
        } else {
            // Magic number 13?
            13
        };
        let millis = thread_rng().gen_range(0, millis_upper);
        config::sleep_millis(millis);

        if let Some(leader) = leader {
            cfg.crash1(leader);
            nup -= 1;
        }

        if nup < 3 {
            let index = thread_rng().gen_range(0, SERVERS);
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

    drop(_guard);
    Ok(())
}

#[test]
fn unreliable_agree() -> config::Result<()> {
    const SERVERS: usize = 5;
    let cfg = Arc::new(config::make_config(SERVERS, true));
    let guard_cfg = cfg.clone();
    let _guard = guard_cfg.deferred_cleanup();

    cfg.begin("Test (2C): unreliable agreement");

    let mut handles = vec![];
    let cfg = Arc::new(cfg);
    for iters in 1..50 {
        for j in 0..4 {
            let cfg = cfg.clone();
            let handle =
                std::thread::spawn(move || cfg.one(100 * iters + j, 1, true));
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

    drop(_guard);
    Ok(())
}

#[test]
fn figure8_unreliable() -> config::Result<()> {
    const SERVERS: usize = 5;
    let cfg = config::make_config(SERVERS, false);
    let _guard = cfg.deferred_cleanup();

    cfg.begin("Test (2C): Figure 8 (unreliable)");

    cfg.one(thread_rng().gen_range(0, 10000), 1, true)?;

    let mut nup = SERVERS;
    for iters in 0..1000 {
        if iters == 200 {
            cfg.set_long_reordering(true);
        }

        let mut leader = None;
        for i in 0..SERVERS {
            if cfg.is_server_alive(i) {
                if let Some(_) = cfg.leader_start(i, thread_rng().gen()) {
                    if cfg.is_connected(i) {
                        leader = Some(i);
                    }
                }
            }
        }

        let millis_upper = if thread_rng().gen_ratio(100, 1000) {
            config::LONG_ELECTION_TIMEOUT_MILLIS >> 1
        } else {
            // Magic number 13?
            13
        };
        let millis = thread_rng().gen_range(0, millis_upper);
        config::sleep_millis(millis);

        if let Some(leader) = leader {
            if thread_rng().gen_ratio(1, 2) {
                cfg.disconnect(leader);
                nup -= 1;
            }
        }

        if nup < 3 {
            let index = thread_rng().gen_range(0, SERVERS);
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

    cfg.one(thread_rng().gen_range(0, 10000), SERVERS, true)?;

    drop(_guard);
    Ok(())
}
