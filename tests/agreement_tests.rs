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
