#[macro_use]
extern crate anyhow;
extern crate kvraft;
extern crate rand;
#[macro_use]
extern crate scopeguard;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;

use rand::{thread_rng, Rng};

use anyhow::Context;
use kvraft::testing_utils::config::{make_config, Config};
use kvraft::Clerk;

fn spawn_clients<T, Func>(
    config: Arc<Config>,
    clients: usize,
    func: Func,
) -> Vec<JoinHandle<T>>
where
    T: 'static + Send,
    Func: 'static + Clone + Send + Sync + Fn(usize, Clerk) -> T,
{
    let mut client_threads = vec![];
    for i in 0..clients {
        let clerk = config.make_clerk();
        let func = func.clone();
        client_threads.push(std::thread::spawn(move || func(i, clerk)))
    }
    eprintln!("spawning clients done.");
    client_threads
}

fn appending_client(
    index: usize,
    mut clerk: Clerk,
    stop: Arc<AtomicBool>,
) -> (usize, String) {
    eprintln!("client {} running.", index);
    let mut op_count = 0usize;
    let key = index.to_string();
    let mut last = String::new();
    let mut rng = thread_rng();

    clerk.put(&key, &last);

    while !stop.load(Ordering::Acquire) {
        eprintln!("client {} starting {}.", index, op_count);
        if rng.gen_ratio(1, 2) {
            let value = format!("({}, {}), ", index, op_count);

            last.push_str(&value);
            clerk.append(&key, &value);

            op_count += 1;
        } else {
            let value = clerk
                .get(&key)
                .expect(&format!("Key {} should exist.", index));
            assert_eq!(value, last);
        }
        eprintln!("client {} done {}.", index, op_count);
    }
    eprintln!("client {} done.", index);
    (op_count, last)
}

fn generic_test(clients: usize, unreliable: bool, maxraftstate: usize) {
    const SERVERS: usize = 5;
    let cfg = Arc::new(make_config(SERVERS, unreliable, maxraftstate));
    // TODO(ditsing): add `defer!(cfg.clean_up());`

    cfg.begin("");
    let mut clerk = cfg.make_clerk();

    const ROUNDS: usize = 3;
    for _ in 0..ROUNDS {
        // Network partition thread.
        let partition_stop = Arc::new(AtomicBool::new(false));
        // KV server clients.
        let clients_stop = Arc::new(AtomicBool::new(false));

        let config = cfg.clone();
        let clients_stop_clone = clients_stop.clone();
        let spawn_client_results = std::thread::spawn(move || {
            spawn_clients(config, clients, move |index: usize, clerk: Clerk| {
                appending_client(index, clerk, clients_stop_clone.clone())
            })
        });

        std::thread::sleep(Duration::from_secs(5));

        // Stop partitions.
        partition_stop.store(true, Ordering::Release);

        // Tell all clients to stop.
        clients_stop.store(true, Ordering::Release);

        let client_results = spawn_client_results
            .join()
            .expect("Spawning clients should never fail.");
        for (index, client_result) in client_results.into_iter().enumerate() {
            let (op_count, last_result) =
                client_result.join().expect("Client should never fail.");
            let real_result = clerk
                .get(index.to_string())
                .expect(&format!("Key {} should exist.", index));
            assert_eq!(real_result, last_result);
            assert!(
                op_count > 10,
                "Client committed only {} operations",
                op_count
            );
        }
    }

    cfg.end();
}

fn check_concurrent_results(
    value: String,
    clients: usize,
    expected: Vec<usize>,
) -> anyhow::Result<()> {
    if !value.starts_with('(') || !value.ends_with(')') {
        bail!("Malformed value string {}", value)
    }
    let inner_value = &value[1..value.len() - 1];
    let mut progress = vec![0; clients];
    for pair_str in inner_value.split(")(") {
        let mut nums = vec![];
        for num_str in pair_str.split(", ") {
            let num: usize = num_str.parse().context(format!(
                "Parsing '{:?}' failed within '{:?}'",
                num_str, value,
            ))?;
            nums.push(num);
        }
        if nums.len() != 2 {
            bail!(
                concat!(
                    "More than two numbers in the same group when",
                    " parsing '{:?}' failed within '{:?}'",
                ),
                pair_str,
                value,
            );
        }
        let (client, curr) = (nums[0], nums[1]);
        if progress[client] != curr {
            bail!(
                "Client {} failed, expecting {}, got {}, others are {:?} in {}",
                client,
                progress[client],
                curr,
                progress,
                value,
            )
        }
        progress[client] = curr + 1;
    }
    assert_eq!(progress, expected, "Expecting progress in {}", value);
    Ok(())
}

#[test]
fn basic_service() {
    generic_test(1, false, 0);
}

#[test]
fn concurrent_client() {
    generic_test(5, false, 0);
}

#[test]
fn unreliable() {
    generic_test(5, true, 0);
}

#[test]
fn unreliable_one_key_many_clients() -> anyhow::Result<()> {
    const SERVERS: usize = 5;
    let cfg = Arc::new(make_config(SERVERS, true, 0));
    let mut clerk = cfg.make_clerk();

    cfg.begin("Test: concurrent append to same key, unreliable (3A)");

    clerk.put("k", "");

    const CLIENTS: usize = 5;
    const ATTEMPTS: usize = 10;
    let client_results = spawn_clients(cfg, CLIENTS, |index, mut clerk| {
        for i in 0..ATTEMPTS {
            clerk.append("k", format!("({}, {})", index, i));
        }
    });
    for client_result in client_results {
        client_result.join().expect("Client should never fail");
    }

    let value = clerk.get("k").expect("Key should exist");

    check_concurrent_results(value, CLIENTS, vec![ATTEMPTS; CLIENTS])
}
