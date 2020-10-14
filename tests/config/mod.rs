use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

pub use anyhow::Result;
use parking_lot::Mutex;
use rand::{thread_rng, Rng};
use tokio::time::Duration;

use ruaft::rpcs::register_server;
use ruaft::utils::DropGuard;
use ruaft::{Raft, RpcClient};

struct ConfigState {
    rafts: Vec<Option<Raft>>,
    connected: Vec<bool>,
}

struct LogState {
    committed_logs: Vec<Vec<i32>>,
    results: Vec<Result<()>>,
    max_index: usize,
}

pub struct Config {
    network: Arc<std::sync::Mutex<labrpc::Network>>,
    server_count: usize,
    state: Mutex<ConfigState>,
    log: Arc<Mutex<LogState>>,
}

impl Config {
    fn server_name(i: usize) -> String {
        format!("ruaft-server-{}", i)
    }

    fn client_name(client: usize, server: usize) -> String {
        format!("ruaft-client-{}-to-{}", client, server)
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

    pub fn check_no_leader(&self) -> Result<()> {
        let state = self.state.lock();
        for i in 0..self.server_count {
            if state.connected[i] {
                if let Some(raft) = &state.rafts[i] {
                    if raft.get_state().1 {
                        bail!(
                            "expected no leader, but {} claims to be leader",
                            i
                        );
                    }
                }
            }
        }
        Ok(())
    }

    pub fn check_terms(&self) -> Result<Option<usize>> {
        let mut term = None;
        let state = self.state.lock();
        for i in 0..self.server_count {
            if state.connected[i] {
                if let Some(raft) = &state.rafts[i] {
                    let raft_term = raft.get_state().0;
                    if let Some(term) = term {
                        if term != raft_term {
                            bail!("Servers disagree on term")
                        }
                    } else {
                        term.replace(raft_term);
                    }
                }
            }
        }
        // Unwrap type Term into usize.
        Ok(term.map(|term| term.0))
    }

    /// Returns the number of peers that committed at least `index` commands,
    /// as well as the command at the index.
    pub fn committed_count(&self, index: usize) -> Result<(usize, i32)> {
        let mut count = 0;
        let mut cmd = Self::INVALID_COMMAND;
        for i in 0..self.server_count {
            let log = self.log.lock();
            if let Err(e) = &log.results[i] {
                bail!(e.to_string())
            }
            if log.committed_logs[i].len() > index {
                let command = log.committed_logs[i][index];
                if count > 0 && command != cmd {
                    bail!(
                        "committed values do not match: index {}, {}, {}",
                        index,
                        cmd,
                        command
                    )
                }
                count += 1;
                cmd = command;
            }
        }
        Ok((count, cmd))
    }

    pub fn wait(
        &self,
        index: usize,
        min_count: usize,
        at_term: Option<usize>,
    ) -> Result<Option<i32>> {
        let mut sleep_time_mills = 10;
        for _ in 0..30 {
            let (count, _) = self.committed_count(index)?;
            if count >= min_count {
                break;
            }
            sleep_millis(sleep_time_mills);
            if sleep_time_mills < 1000 {
                sleep_time_mills <<= 1;
            }

            if let Some(at_term) = at_term {
                let state = self.state.lock();
                for raft in &state.rafts {
                    if let Some(raft) = raft {
                        let (term, _) = raft.get_state();
                        if term.0 > at_term {
                            return Ok(None);
                        }
                    }
                }
            }
        }

        let (count, cmd) = self.committed_count(index)?;
        if count < min_count {
            bail!(
                "only {} decided for index {}; wanted {}",
                count,
                index,
                min_count
            )
        }
        Ok(Some(cmd))
    }

    pub fn one(
        &self,
        cmd: i32,
        expected_servers: usize,
        retry: bool,
    ) -> Result<usize> {
        let start = Instant::now();
        let mut cnt = 0;
        while start.elapsed() < Duration::from_secs(10) {
            let mut first_index = None;
            for _ in 0..self.server_count {
                cnt += 1;
                cnt %= self.server_count;
                let state = self.state.lock();
                if state.connected[cnt] {
                    if let Some(raft) = &state.rafts[cnt] {
                        if let Some((_, index)) =
                            raft.start(ruaft::Command(cmd))
                        {
                            first_index.replace(index);
                        }
                    }
                }
            }

            if let Some(index) = first_index {
                let agreement_start = Instant::now();
                while agreement_start.elapsed() < Duration::from_secs(2) {
                    let (commit_count, committed_command) =
                        self.committed_count(index)?;
                    if commit_count > 0
                        && commit_count >= expected_servers
                        && committed_command == cmd
                    {
                        return Ok(index);
                    }
                    sleep_millis(20);
                }
                if !retry {
                    break;
                }
            } else {
                sleep_millis(50);
            }
        }
        Err(anyhow!("one({}) failed to reach agreement", cmd))
    }

    pub fn connect(&self, index: usize) {
        self.set_connect(index, true);
    }

    pub fn disconnect(&self, index: usize) {
        self.set_connect(index, false);
    }

    pub fn set_connect(&self, index: usize, yes: bool) {
        let mut state = self.state.lock();
        state.connected[index] = yes;

        let mut network = unlock(&self.network);

        // Outgoing clients.
        for j in 0..self.server_count {
            if state.connected[j] {
                network.set_enable_client(Self::client_name(index, j), yes)
            }
        }

        // Incoming clients.
        for j in 0..self.server_count {
            if state.connected[j] {
                network.set_enable_client(Self::client_name(j, index), yes);
            }
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

    pub fn start1(&mut self, index: usize) -> Result<()> {
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

        let log_clone = self.log.clone();
        let raft = Raft::new(clients, index, move |cmd_index, cmd| {
            Self::apply_command(log_clone.clone(), index, cmd_index, cmd.0)
        });
        self.state.lock().rafts[index].replace(raft.clone());

        let raft = Arc::new(raft);
        register_server(raft, Self::server_name(index), self.network.as_ref())?;
        Ok(())
    }

    /// Start a new command, returns (term, index).
    pub fn leader_start(
        &self,
        leader: usize,
        cmd: i32,
    ) -> Option<(usize, usize)> {
        self.state.lock().rafts[leader]
            .as_ref()
            .map(|raft| {
                raft.start(ruaft::Command(cmd))
                    .map(|(term, index)| (term.0, index))
            })
            .unwrap()
    }

    pub fn total_rpcs(&self) -> usize {
        unlock(&self.network).get_total_rpc_count()
    }

    pub fn end(&self) {}

    pub fn cleanup(&self) {
        let mut network = unlock(&self.network);
        for i in 0..self.server_count {
            network.remove_server(Self::server_name(i));
        }
        for raft in &mut self.state.lock().rafts {
            if let Some(raft) = raft.take() {
                raft.kill();
            }
        }
    }

    pub fn deferred_cleanup(&self) -> impl Drop + '_ {
        DropGuard::new(move || self.cleanup())
    }
}

impl Config {
    const INVALID_COMMAND: i32 = -1;

    fn apply_command(
        log_state: Arc<Mutex<LogState>>,
        server_index: usize,
        index: usize,
        command: i32,
    ) {
        let mut log_state = log_state.lock();
        let committed_logs = &mut log_state.committed_logs;
        let mut err = None;
        for (one_index, one_server) in committed_logs.iter().enumerate() {
            if one_server.len() > index && one_server[index] != command {
                err = Some((
                    one_index,
                    Err(anyhow!(
                        "commit index ={} server={} {} != server={} {}",
                        index,
                        server_index,
                        command,
                        one_index,
                        one_server[index],
                    )),
                ));
                break;
            }
        }

        let one_server = &mut committed_logs[server_index];
        if one_server.len() <= index {
            one_server.resize(index + 1, Self::INVALID_COMMAND);
        }
        one_server[index] = command;

        if index > 1 && one_server[index - 1] == Self::INVALID_COMMAND {
            log_state.results[server_index] = Err(anyhow!(
                "server {} apply out of order {}",
                server_index,
                index
            ));
        } else if let Some((one_index, err)) = err {
            log_state.results[one_index] = err
        }

        if index > log_state.max_index {
            log_state.max_index = index;
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
    let log = Arc::new(Mutex::new(LogState {
        committed_logs: vec![vec![]; server_count],
        results: vec![],
        max_index: 0,
    }));
    log.lock().results.resize_with(server_count, || Ok(()));
    let mut cfg = Config {
        network,
        server_count,
        state,
        log,
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
