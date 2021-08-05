use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use rand::{thread_rng, Rng};
use test_utils::thread_local_logger::LocalLogger;

use linearizability::{KvInput, KvModel, KvOp, KvOutput, Operation};

use super::config::{make_config, Config};
use crate::utils::{
    sleep_election_timeouts, sleep_millis, LONG_ELECTION_TIMEOUT_MILLIS,
};
use kvraft::Clerk;

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
    log::debug!("spawning clients done.");
    client_threads
}

fn appending_client(
    index: usize,
    mut clerk: Clerk,
    stop: Arc<AtomicBool>,
) -> (usize, String) {
    log::info!("client {} running.", index);
    let mut op_count = 0usize;
    let key = index.to_string();
    let mut last = String::new();
    let mut rng = thread_rng();

    clerk.put(&key, &last);

    while !stop.load(Ordering::Acquire) {
        log::debug!("client {} starting {}.", index, op_count);
        if rng.gen_ratio(1, 2) {
            let value = format!("({}, {}), ", index, op_count);

            last.push_str(&value);
            clerk.append(&key, &value);

            op_count += 1;
        } else {
            let value = clerk
                .get(&key)
                .unwrap_or_else(|| panic!("Key {} should exist.", index));
            assert_eq!(value, last);
        }
        log::debug!("client {} done {}.", index, op_count);
    }
    log::info!("client {} done.", index);
    (op_count, last)
}

fn linearizability_client(
    index: usize,
    client_count: usize,
    mut clerk: Clerk,
    stop: Arc<AtomicBool>,
    ops: Arc<Mutex<Vec<Operation<KvInput, KvOutput>>>>,
) -> (usize, String) {
    let mut op_count = 0usize;
    while !stop.load(Ordering::Acquire) {
        let key = thread_rng().gen_range(0..client_count).to_string();
        let value = format!("({}, {}), ", index, op_count);
        let call_time = Instant::now();
        let call_op;
        let return_op;
        if thread_rng().gen_ratio(500, 1000) {
            clerk.append(&key, &value);
            call_op = KvInput {
                op: KvOp::Append,
                key,
                value,
            };
            return_op = KvOutput::default();
        } else if thread_rng().gen_ratio(100, 1000) {
            clerk.put(&key, &value);
            call_op = KvInput {
                op: KvOp::Put,
                key,
                value,
            };
            return_op = KvOutput::default();
        } else {
            let result = clerk.get(&key).unwrap_or_default();
            call_op = KvInput {
                op: KvOp::Get,
                key,
                value: Default::default(),
            };
            return_op = result;
        }
        let return_time = Instant::now();
        ops.lock().push(Operation {
            call_op,
            call_time,
            return_op,
            return_time,
        });

        op_count += 1;
    }
    (op_count, String::new())
}

const PARTITION_MAX_DELAY_MILLIS: u64 = 200;

fn run_partition(cfg: Arc<Config>, stop: Arc<AtomicBool>) {
    while !stop.load(Ordering::Acquire) {
        cfg.random_partition();
        let delay = thread_rng().gen_range(
            LONG_ELECTION_TIMEOUT_MILLIS
                ..LONG_ELECTION_TIMEOUT_MILLIS + PARTITION_MAX_DELAY_MILLIS,
        );
        sleep_millis(delay);
    }
}

#[derive(Debug)]
struct Laps {
    clients_started: Duration,
    partition_done: Duration,
    crash_done: Duration,
    running_time: Duration,
    partition_stopped: Duration,
    client_spawn: Duration,
    client_waits: Duration,
}

#[derive(Default)]
pub struct GenericTestParams {
    pub clients: usize,
    pub unreliable: bool,
    pub partition: bool,
    pub crash: bool,
    pub maxraftstate: Option<usize>,
    pub min_ops: Option<usize>,
    pub test_linearizability: bool,
}

pub fn generic_test(test_params: GenericTestParams) {
    let GenericTestParams {
        clients,
        unreliable,
        partition,
        crash,
        maxraftstate,
        min_ops,
        test_linearizability,
    } = test_params;
    let maxraftstate = maxraftstate.unwrap_or(usize::MAX);
    let min_ops = min_ops.unwrap_or(10);
    let servers: usize = if test_linearizability { 7 } else { 5 };
    let cfg = Arc::new(make_config(servers, unreliable, maxraftstate));

    cfg.begin("");
    let mut clerk = cfg.make_clerk();
    let ops = Arc::new(Mutex::new(vec![]));

    let mut laps = vec![];
    const ROUNDS: usize = 3;
    for round in 0..ROUNDS {
        log::info!("Running round {}", round);
        let start = Instant::now();
        // Network partition thread.
        let partition_stop = Arc::new(AtomicBool::new(false));
        // KV server clients.
        let clients_stop = Arc::new(AtomicBool::new(false));

        let config = cfg.clone();
        let clients_stop_clone = clients_stop.clone();
        let ops_clone = ops.clone();
        let logger = LocalLogger::inherit();
        let spawn_client_results = std::thread::spawn(move || {
            logger.clone().attach();
            spawn_clients(config, clients, move |index: usize, clerk: Clerk| {
                logger.clone().attach();
                if !test_linearizability {
                    appending_client(index, clerk, clients_stop_clone.clone())
                } else {
                    linearizability_client(
                        index,
                        clients,
                        clerk,
                        clients_stop_clone.clone(),
                        ops_clone.clone(),
                    )
                }
            })
        });
        let clients_started = start.elapsed();

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
        let partition_done = start.elapsed();

        if crash {
            cfg.crash_all();
            sleep_election_timeouts(1);
            cfg.restart_all();
        }
        let crash_done = start.elapsed();

        std::thread::sleep(Duration::from_secs(5));
        let running_time = start.elapsed();

        // Stop partitions.
        partition_stop.store(true, Ordering::Release);
        if let Some(result) = partition_result {
            result.join().expect("Partition thread should never fail");
            cfg.connect_all();
            sleep_election_timeouts(1);
        }
        let partition_stopped = start.elapsed();

        // Tell all clients to stop.
        clients_stop.store(true, Ordering::Release);

        let client_results = spawn_client_results
            .join()
            .expect("Spawning clients should never fail.");
        let client_spawn = start.elapsed();
        for (index, client_result) in client_results.into_iter().enumerate() {
            let (op_count, last_result) =
                client_result.join().expect("Client should never fail");
            if !last_result.is_empty() {
                let real_result = clerk
                    .get(index.to_string())
                    .unwrap_or_else(|| panic!("Key {} should exist.", index));
                assert_eq!(real_result, last_result);
            }
            log::info!("Client {} committed {} operations", index, op_count);
            assert!(
                op_count >= min_ops,
                "Client {} committed {} operations, less than {}",
                index,
                op_count,
                min_ops
            );
        }
        let client_waits = start.elapsed();
        laps.push(Laps {
            clients_started,
            partition_done,
            crash_done,
            running_time,
            partition_stopped,
            client_spawn,
            client_waits,
        });
    }

    cfg.end();
    cfg.clean_up();

    for (index, laps) in laps.iter().enumerate() {
        log::info!("Round {} diagnostics: {:?}", index, laps);
    }

    if test_linearizability {
        let ops: &'static Vec<Operation<KvInput, KvOutput>> =
            Box::leak(Box::new(
                Arc::try_unwrap(ops)
                    .expect("No one should be holding ops")
                    .into_inner(),
            ));
        let start = Instant::now();
        log::info!("Searching for linearization arrangements ...");
        assert!(
            linearizability::check_operations_timeout::<KvModel>(&ops, None),
            "History {:?} is not linearizable,",
            ops,
        );
        log::info!(
            "Searching for linearization arrangements done after {:?}.",
            start.elapsed()
        );
    }
}
