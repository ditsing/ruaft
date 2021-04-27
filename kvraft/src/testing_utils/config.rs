use std::sync::Arc;

pub use anyhow::Result;
use labrpc::Network;
use parking_lot::Mutex;
use rand::seq::SliceRandom;
use rand::thread_rng;

use ruaft::rpcs::register_server;
use ruaft::RpcClient;

use crate::client::Clerk;
use crate::server::KVServer;
use crate::testing_utils::memory_persister::MemoryStorage;
use crate::testing_utils::rpcs::register_kv_server;

struct ConfigState {
    kv_servers: Vec<Option<Arc<KVServer>>>,
    next_clerk: usize,
}

pub struct Config {
    network: Arc<Mutex<labrpc::Network>>,
    server_count: usize,
    state: Mutex<ConfigState>,
    storage: Mutex<MemoryStorage>,
    maxraftstate: usize,
}

impl Config {
    fn kv_clerk_name(i: usize, server: usize) -> String {
        format!("kvraft-clerk-client-{}-to-{}", i, server)
    }

    fn kv_server_name(i: usize) -> String {
        format!("kv-server-{}", i)
    }

    fn server_name(i: usize) -> String {
        format!("kvraft-server-{}", i)
    }

    fn client_name(client: usize, server: usize) -> String {
        format!("kvraft-client-{}-to-{}", client, server)
    }

    fn start_server(&self, index: usize) -> Result<()> {
        let mut clients = vec![];
        {
            let mut network = self.network.lock();
            for j in 0..self.server_count {
                clients.push(RpcClient::new(network.make_client(
                    Self::client_name(index, j),
                    Self::server_name(j),
                )))
            }
        }

        let persister = self.storage.lock().at(index);

        let kv = KVServer::new(clients, index, persister);
        self.state.lock().kv_servers[index].replace(kv.clone());

        let raft = std::rc::Rc::new(kv.raft());

        register_server(raft, Self::server_name(index), self.network.as_ref())?;

        register_kv_server(
            kv,
            Self::kv_server_name(index),
            self.network.as_ref(),
        )?;
        Ok(())
    }

    pub fn begin<S: std::fmt::Display>(&self, msg: S) {
        eprintln!("{}", msg);
    }

    fn shuffled_indexes(&self) -> Vec<usize> {
        let mut indexes: Vec<usize> = (0..self.server_count).collect();
        indexes.shuffle(&mut thread_rng());
        indexes
    }

    pub fn partition(&self) -> (Vec<usize>, Vec<usize>) {
        let state = self.state.lock();
        let mut indexes = self.shuffled_indexes();

        // Swap leader to position 0.
        let leader_position = indexes
            .iter()
            .position(|index| {
                state.kv_servers[*index]
                    .as_ref()
                    .map_or(false, |kv| kv.raft().get_state().1)
            })
            .unwrap_or(0);
        indexes.swap(0, leader_position);

        let part_one = indexes.split_off(indexes.len() / 2);
        let part_two = indexes;
        self.network_partition(&part_one, &part_two);

        (part_one, part_two)
    }

    pub fn random_partition(&self) -> (Vec<usize>, Vec<usize>) {
        let mut indexes = self.shuffled_indexes();
        let part_one = indexes.split_off(indexes.len() / 2);
        let part_two = indexes;
        self.network_partition(&part_one, &part_two);

        (part_one, part_two)
    }

    fn set_connect(
        network: &mut Network,
        from: &[usize],
        to: &[usize],
        yes: bool,
    ) {
        for i in from {
            for j in to {
                network.set_enable_client(Self::client_name(*i, *j), yes)
            }
        }
    }

    fn network_partition(&self, part_one: &[usize], part_two: &[usize]) {
        let mut network = self.network.lock();
        Self::set_connect(&mut network, part_one, part_two, false);
        Self::set_connect(&mut network, part_two, part_one, false);
        Self::set_connect(&mut network, part_one, part_one, true);
        Self::set_connect(&mut network, part_two, part_two, true);
    }

    pub fn connect_all(&self) {
        let all: Vec<usize> = (0..self.state.lock().kv_servers.len()).collect();
        let mut network = self.network.lock();
        Self::set_connect(&mut network, &all, &all, true);
    }

    fn crash_server(&self, index: usize) {
        {
            let all: Vec<usize> = (0..self.server_count).collect();

            let mut network = self.network.lock();
            Self::set_connect(&mut network, &all, &[index], false);
            Self::set_connect(&mut network, &[index], &all, false);

            network.remove_server(Self::server_name(index));
            network.remove_server(Self::kv_server_name(index));
        }

        let data = self.storage.lock().at(index).read();

        let persister = self.storage.lock().replace(index);
        persister.restore(data);

        if let Some(kv_server) = self.state.lock().kv_servers[index].take() {
            kv_server.kill();
        }
    }

    pub fn crash_all(&self) {
        for i in 0..self.server_count {
            self.crash_server(i);
        }
    }

    pub fn restart_all(&self) {
        for index in 0..self.server_count {
            self.start_server(index)
                .expect("Start server should never fail");
        }
    }

    fn set_clerk_connect(
        network: &mut Network,
        clerk_index: usize,
        to: &[usize],
        yes: bool,
    ) {
        for j in to {
            network.set_enable_client(Self::kv_clerk_name(clerk_index, *j), yes)
        }
    }

    pub fn make_limited_clerk(&self, to: &[usize]) -> Clerk {
        let mut clients = vec![];
        let clerk_index = {
            let mut state = self.state.lock();
            state.next_clerk += 1;
            state.next_clerk
        };

        {
            let mut network = self.network.lock();
            for j in 0..self.server_count {
                clients.push(network.make_client(
                    Self::kv_clerk_name(clerk_index, j),
                    Self::kv_server_name(j),
                ));
            }
            // Disable clerk connection to all kv servers.
            Self::set_clerk_connect(
                &mut network,
                clerk_index,
                &(0..self.server_count).collect::<Vec<usize>>(),
                false,
            );
            // Enable clerk connection to some servers.
            Self::set_clerk_connect(&mut network, clerk_index, to, true);
        }

        clients.shuffle(&mut thread_rng());
        Clerk::new(clients)
    }

    pub fn make_clerk(&self) -> Clerk {
        self.make_limited_clerk(&(0..self.server_count).collect::<Vec<usize>>())
    }

    pub fn connect_all_clerks(&self) {
        let mut network = self.network.lock();
        let all = &(0..self.server_count).collect::<Vec<usize>>();
        for clerk_index in 0..self.state.lock().next_clerk {
            Self::set_clerk_connect(&mut network, clerk_index + 1, all, true);
        }
    }

    pub fn end(&self) {}

    pub fn clean_up(&self) {
        let mut network = self.network.lock();
        for i in 0..self.server_count {
            network.remove_server(Self::server_name(i));
            network.remove_server(Self::kv_server_name(i));
        }
        network.stop();
        drop(network);

        for kv_server in &mut self.state.lock().kv_servers {
            if let Some(kv_server) = kv_server.take() {
                kv_server.kill();
            }
        }
    }
}

pub fn make_config(
    server_count: usize,
    unreliable: bool,
    maxraftstate: usize,
) -> Config {
    let network = labrpc::Network::run_daemon();
    {
        let mut unlocked_network = network.lock();
        unlocked_network.set_reliable(!unreliable);
        unlocked_network.set_long_delays(true);
    }

    let state = Mutex::new(ConfigState {
        kv_servers: vec![None; server_count],
        next_clerk: 0,
    });

    let mut storage = MemoryStorage::default();
    for _ in 0..server_count {
        storage.make();
    }
    let storage = Mutex::new(storage);

    let cfg = Config {
        network,
        server_count,
        state,
        storage,
        maxraftstate,
    };

    for i in 0..server_count {
        cfg.start_server(i)
            .expect("Starting server should not fail");
    }

    cfg
}

pub fn sleep_millis(mills: u64) {
    std::thread::sleep(std::time::Duration::from_millis(mills))
}

pub const LONG_ELECTION_TIMEOUT_MILLIS: u64 = 1000;
pub fn sleep_election_timeouts(count: u64) {
    sleep_millis(LONG_ELECTION_TIMEOUT_MILLIS * count)
}
