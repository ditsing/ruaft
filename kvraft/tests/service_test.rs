extern crate kvraft;
extern crate rand;
#[macro_use]
extern crate scopeguard;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;

use rand::{thread_rng, Rng};

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

    clerk.put(key.clone(), last.clone());

    while !stop.load(Ordering::Acquire) {
        eprintln!("client {} starting {}.", index, op_count);
        if rng.gen_ratio(1, 2) {
            let value = format!("({}, {}), ", index, op_count);

            last.push_str(&value);
            clerk.append(key.clone(), value);

            op_count += 1;
        } else {
            let value = clerk
                .get(key.clone())
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

#[test]
fn basic_service() {
    generic_test(1, false, 0);
}
