use std::sync::Arc;

pub use anyhow::Result;
use labrpc::Network;
use parking_lot::Mutex;
use rand::seq::SliceRandom;
use rand::thread_rng;

use client::Clerk;
use ruaft::rpcs::register_server;
use ruaft::RpcClient;
use server::KVServer;
use testing_utils::memory_persister::MemoryStorage;
use testing_utils::rpcs::register_kv_server;

struct ConfigState {
    kv_servers: Vec<Option<Arc<KVServer>>>,
    next_clerk: usize,
}

pub struct Config {
    network: Arc<Mutex<labrpc::Network>>,
    server_count: usize,
    state: Mutex<ConfigState>,
    storage: MemoryStorage,
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

        let persister = self.storage.at(index);

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

    fn make_partition(&self) -> (Vec<usize>, Vec<usize>) {
        let state = self.state.lock();
        let mut indexes: Vec<usize> = (0..state.kv_servers.len()).collect();
        indexes.shuffle(&mut thread_rng());

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

        (indexes.split_off(state.kv_servers.len() / 2), indexes)
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

    pub fn partition(&self, part_one: &[usize], part_two: &[usize]) {
        let mut network = self.network.lock();
        Self::set_connect(&mut network, part_one, part_two, false);
        Self::set_connect(&mut network, part_two, part_one, false);
        Self::set_connect(&mut network, part_one, part_one, true);
        Self::set_connect(&mut network, part_two, part_two, true);
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
            if let Some(kv) = kv_server.take() {
                match Arc::try_unwrap(kv) {
                    Ok(kv) => kv.kill(),
                    Err(_) => panic!(
                        "All references to KVServer should have been dropped"
                    ),
                }
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
