use parking_lot::Mutex;
use rand::{thread_rng, Rng};
use ruaft::rpcs::register_server;
use ruaft::{Raft, RpcClient};
use std::collections::HashMap;
use std::sync::Arc;

struct ConfigState {
    rafts: Vec<Option<Raft>>,
    connected: Vec<bool>,
}

pub struct Config {
    network: Arc<std::sync::Mutex<labrpc::Network>>,
    server_count: usize,
    state: Mutex<ConfigState>,
}

pub use anyhow::Result;

impl Config {
    fn server_name(i: usize) -> String {
        format!("ruaft-server-{}", i)
    }

    fn client_name(client: usize, server: usize) -> String {
        format!("ruaft-server-{}-to-{}", client, server)
    }

    pub fn begin<S: std::fmt::Display>(&self, msg: S) {
        eprintln!("{}", msg);
    }

    pub fn check_one_leader(&self) -> Result<usize> {
        for _ in 0..10 {
            let millis = 450 + thread_rng().gen_range(0, 100);
            sleep_millis(millis);

            let mut leaders = HashMap::new();
            let state = self.state.lock();
            for i in 0..self.server_count {
                if state.connected[i] {
                    if let Some(raft) = &state.rafts[i] {
                        let (term, is_leader) = raft.get_state();
                        if is_leader {
                            leaders.entry(term.0).or_insert(vec![]).push(i)
                        }
                    }
                }
            }

            let mut last_term_with_leader = 0;
            let mut last_leader = 0;
            for (term, leaders) in leaders {
                if leaders.len() > 1 {
                    bail!("term {} has {} (>1) leaders", term, leaders.len());
                }
                if term > last_term_with_leader {
                    last_term_with_leader = term;
                    last_leader = leaders[0];
                }
            }

            if last_term_with_leader != 0 {
                return Ok(last_leader);
            }
        }
        Err(anyhow!("expected one leader, got none"))
    }

    pub fn check_terms(&self) -> std::io::Result<()> {
        Ok(())
    }

    pub fn connect(&self, index: usize) {
        self.set_connect(index, true);
    }

    pub fn disconnect(&self, index: usize) {
        self.set_connect(index, false);
    }

    pub fn set_connect(&self, index: usize, yes: bool) {
        self.state.lock().connected[index] = yes;

        let mut network = unlock(&self.network);

        // Outgoing clients.
        for j in 0..self.server_count {
            network.set_enable_client(Self::client_name(index, j), yes)
        }

        // Incoming clients.
        for j in 0..self.server_count {
            network.set_enable_client(Self::client_name(j, index), yes);
        }
    }

    pub fn crash1(&mut self, index: usize) {
        self.disconnect(index);

        unlock(self.network.as_ref()).remove_server(Self::server_name(index));
        let raft = {
            let mut state = self.state.lock();
            state.rafts[index].take()
        };
        if let Some(raft) = raft {
            raft.kill();
        }
    }

    pub fn start1(&mut self, index: usize) -> std::io::Result<()> {
        if self.state.lock().rafts[index].is_some() {
            self.crash1(index);
        }

        let mut clients = vec![];
        {
            let mut network = unlock(&self.network);
            for j in 0..self.server_count {
                clients.push(RpcClient::new(network.make_client(
                    Self::client_name(index, j),
                    Self::server_name(j),
                )))
            }
        }

        let raft = Raft::new(clients, index, |_, _| {});
        self.state.lock().rafts[index].replace(raft.clone());

        let raft = Arc::new(raft);
        register_server(raft, Self::server_name(index), self.network.as_ref())
    }

    pub fn end(&self) {}

    pub fn cleanup(&self) {
        for raft in &mut self.state.lock().rafts {
            if let Some(_raft) = raft.take() {
                raft.kill();
            }
        }
    }
}

fn unlock<T>(locked: &std::sync::Mutex<T>) -> std::sync::MutexGuard<T> {
    locked.lock().expect("Unlocking network should not fail")
}

pub fn make_config(server_count: usize, unreliable: bool) -> Config {
    let network = labrpc::Network::run_daemon();
    {
        let mut unlocked_network = unlock(&network);
        unlocked_network.set_reliable(!unreliable);
        unlocked_network.set_long_delays(true);
    }

    let state = Mutex::new(ConfigState {
        rafts: vec![None; server_count],
        connected: vec![true; server_count],
    });
    let mut cfg = Config {
        network,
        server_count,
        state,
    };

    for i in 0..server_count {
        cfg.start1(i).expect("Starting server should not fail");
    }

    cfg
}

pub fn sleep_millis(mills: u64) {
    std::thread::sleep(std::time::Duration::from_millis(mills))
}

const LONG_ELECTION_TIMEOUT_MILLIS: u64 = 1000;
pub fn sleep_election_timeouts(count: u64) {
    sleep_millis(LONG_ELECTION_TIMEOUT_MILLIS * count)
}
