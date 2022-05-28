use std::sync::Arc;

use labrpc::Network;
use parking_lot::Mutex;
use rand::seq::SliceRandom;
use rand::thread_rng;

use kvraft::Clerk;
use kvraft::KVServer;

use crate::{register_kv_server, register_server, Persister, RpcClient};

struct ConfigState {
    kv_servers: Vec<Option<Arc<KVServer>>>,
    next_clerk: usize,
}

pub struct Config {
    network: Arc<Mutex<labrpc::Network>>,
    server_count: usize,
    state: Mutex<ConfigState>,
    storage: Mutex<Vec<Arc<Persister>>>,
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

    fn start_server(&self, index: usize) -> std::io::Result<()> {
        let mut clients = vec![];
        {
            let mut network = self.network.lock();
            for j in 0..self.server_count {
                clients.push(crate::RpcClient::new(network.make_client(
                    Self::client_name(index, j),
                    Self::server_name(j),
                )))
            }
        }

        let persister = self.storage.lock()[index].clone();

        let kv =
            KVServer::new(clients, index, persister, Some(self.maxraftstate));
        self.state.lock().kv_servers[index].replace(kv.clone());

        let raft = Arc::new(kv.raft().clone());

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

    pub fn network_partition(&self, part_one: &[usize], part_two: &[usize]) {
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

        let data = self.storage.lock()[index].read();

        let persister = Arc::new(Persister::new());
        self.storage.lock()[index] = persister.clone();
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
                clients.push(RpcClient::new(network.make_client(
                    Self::kv_clerk_name(clerk_index, j),
                    Self::kv_server_name(j),
                )));
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

impl Config {
    fn check_size(
        &self,
        upper: usize,
        size_fn: impl Fn(&Persister) -> usize,
    ) -> Result<(), String> {
        let mut over_limits = String::new();
        for (index, p) in self.storage.lock().iter().enumerate() {
            let size = size_fn(p);
            if size > upper {
                let str = format!(" (index {}, size {})", index, size);
                over_limits.push_str(&str);
            }
        }
        if !over_limits.is_empty() {
            return Err(format!(
                "logs were not trimmed to {}:{}",
                upper, over_limits
            ));
        }
        Ok(())
    }

    pub fn check_log_size(&self, upper: usize) -> Result<(), String> {
        self.check_size(upper, ruaft::Persister::state_size)
    }

    pub fn check_snapshot_size(&self, upper: usize) -> Result<(), String> {
        self.check_size(upper, Persister::snapshot_size)
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

    let storage = Mutex::new(vec![]);
    storage
        .lock()
        .resize_with(server_count, || Arc::new(Persister::new()));

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
