#[macro_use]
extern crate anyhow;
extern crate bytes;
extern crate labrpc;
extern crate ruaft;

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

    cfg.one(12, SERVERS, true);

    let leader1 = cfg.check_one_leader()?;
    cfg.disconnect(leader1);
    cfg.start1(leader1)?;
    cfg.connect(leader1);

    cfg.one(13, SERVERS, true)?;

    let leader2 = cfg.check_one_leader()?;
    cfg.disconnect(leader2);
    cfg.one(14, SERVERS - 1, true);
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
