#[macro_use]
extern crate anyhow;
extern crate bytes;
extern crate labrpc;
extern crate ruaft;
#[macro_use]
extern crate scopeguard;

mod config;

#[test]
fn initial_election() -> config::Result<()> {
    const SERVERS: usize = 3;
    let cfg = config::make_config(SERVERS, false);
    defer!(cfg.cleanup());

    cfg.begin("Test (2A): initial election");

    cfg.check_one_leader()?;

    config::sleep_millis(50);

    let first_term = cfg.check_terms()?;
    config::sleep_election_timeouts(2);

    let second_term = cfg.check_terms()?;

    if first_term != second_term {
        eprintln!("Warning: term change even though there were no failures");
    }

    cfg.check_one_leader()?;

    cfg.end();
    Ok(())
}

#[test]
fn re_election() -> config::Result<()> {
    const SERVERS: usize = 3;
    let cfg = config::make_config(SERVERS, false);
    defer!(cfg.cleanup());

    cfg.begin("Test (2A): election after network failure");

    let leader_one = cfg.check_one_leader()?;

    cfg.disconnect(leader_one);
    cfg.check_one_leader()?;

    cfg.connect(leader_one);

    let leader_two = cfg.check_one_leader()?;
    let other = (leader_two + 1) % SERVERS;

    cfg.disconnect(leader_two);
    cfg.disconnect(other);
    cfg.check_no_leader()?;

    cfg.connect(other);
    cfg.check_one_leader()?;

    cfg.connect(leader_two);
    cfg.check_one_leader()?;

    cfg.end();
    Ok(())
}
