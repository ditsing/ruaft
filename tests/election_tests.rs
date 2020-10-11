extern crate ruaft;

mod config;

#[test]
fn initial_election() -> std::io::Result<()> {
    const SERVERS: usize = 3;
    let cfg = config::make_config(SERVERS, false);
    let guard = ruaft::utils::DropGuard::new(|| cfg.cleanup());

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

    drop(guard);
    Ok(())
}

#[test]
fn re_election() {}
