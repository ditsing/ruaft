use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;

use rand::{thread_rng, Rng};

use crate::testing_utils::config::{
    make_config, sleep_election_timeouts, sleep_millis, Config,
    LONG_ELECTION_TIMEOUT_MILLIS,
};
use crate::Clerk;

pub fn spawn_clients<T, Func>(
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

const PARTITION_MAX_DELAY_MILLIS: u64 = 200;

fn run_partition(cfg: Arc<Config>, stop: Arc<AtomicBool>) {
    while !stop.load(Ordering::Acquire) {
        cfg.random_partition();
        let delay = thread_rng().gen_range(
            LONG_ELECTION_TIMEOUT_MILLIS
                ..LONG_ELECTION_TIMEOUT_MILLIS + PARTITION_MAX_DELAY_MILLIS,
        );
        std::thread::sleep(Duration::from_millis(delay));
    }
}

#[derive(Default)]
pub struct GenericTestParams {
    pub clients: usize,
    pub unreliable: bool,
    pub partition: bool,
    pub crash: bool,
    pub maxraftstate: Option<usize>,
    pub min_ops: Option<usize>,
}

pub fn generic_test(test_params: GenericTestParams) {
    let GenericTestParams {
        clients,
        unreliable,
        partition,
        crash,
        maxraftstate,
        min_ops,
    } = test_params;
    let maxraftstate = maxraftstate.unwrap_or(usize::MAX);
    let min_ops = min_ops.unwrap_or(10);
    const SERVERS: usize = 5;
    let cfg = Arc::new(make_config(SERVERS, unreliable, maxraftstate));
    defer!(cfg.clean_up());

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

        let partition_result = if partition {
            // Let the clients perform some operations without interruption.
            sleep_millis(1000);
            let config = cfg.clone();
            let partition_stop_clone = partition_stop.clone();
            Some(std::thread::spawn(|| {
                run_partition(config, partition_stop_clone)
            }))
        } else {
            None
        };

        if crash {
            cfg.crash_all();
            sleep_election_timeouts(1);
            cfg.restart_all();
        }

        std::thread::sleep(Duration::from_secs(5));

        // Stop partitions.
        partition_stop.store(true, Ordering::Release);
        partition_result.map(|result| {
            result.join().expect("Partition thread should never fail");
            cfg.connect_all();
            sleep_election_timeouts(1);
        });

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
            eprintln!("Client {} committed {} operations", index, op_count);
            assert!(
                op_count >= min_ops,
                "Client committed less than {} operations",
                min_ops
            );
        }
    }

    cfg.end();
}
