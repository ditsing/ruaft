use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::Sender;
use std::sync::Arc;

use crate::kv_service::connect_to_kv_service;
use kvraft::Clerk;

pub(crate) fn create_clerk(socket_addrs: Vec<SocketAddr>) -> OneClerk {
    OneClerk::create(socket_addrs)
}

#[derive(Clone)]
pub(crate) struct OneClerk {
    ready: Arc<AtomicBool>,
    requests: crossbeam_channel::Sender<(ClerkRequest, Sender<String>)>,
}

enum ClerkRequest {
    Get(String),
    Put(String, String),
    Append(String, String),
}

impl OneClerk {
    pub(crate) fn create(socket_addrs: Vec<SocketAddr>) -> Self {
        let ready = Arc::new(AtomicBool::new(false));
        // Create a thread that blocks on all requests to the clerk.
        let requests = Self::run_clerk_thread(socket_addrs, ready.clone());

        OneClerk { ready, requests }
    }

    async fn initialize_clerk(socket_addrs: Vec<SocketAddr>) -> Clerk {
        log::info!("Starting clerk creation ...");
        let mut clients = vec![None; socket_addrs.len()];
        while clients.iter().filter(|e| e.is_none()).count() != 0 {
            for (index, socket_addr) in socket_addrs.iter().enumerate() {
                let result = connect_to_kv_service(socket_addr.clone()).await;
                match result {
                    Ok(client) => clients[index] = Some(client),
                    Err(e) => log::error!(
                        "Error connecting to {:?}: {}",
                        socket_addr,
                        e
                    ),
                }
            }
            log::info!("Clerk clients are {:?}", clients);
        }

        log::info!("Done clerk creation ...");
        let clients = clients.into_iter().map(|e| e.unwrap()).collect();
        Clerk::new(clients)
    }

    /// A thread must be created for get requests. We cannot run the blocking
    /// Clerk functions on tokio thread pool threads.
    fn run_clerk_thread(
        socket_addrs: Vec<SocketAddr>,
        ready: Arc<AtomicBool>,
    ) -> crossbeam_channel::Sender<(ClerkRequest, Sender<String>)> {
        let local_logger =
            test_utils::thread_local_logger::LocalLogger::inherit();

        // Steal a tokio runtime to run the initializer.
        let tokio_handle = tokio::runtime::Handle::current();
        let (tx, rx) =
            crossbeam_channel::unbounded::<(ClerkRequest, Sender<String>)>();
        std::thread::spawn(move || {
            local_logger.attach();

            let mut clerk =
                tokio_handle.block_on(Self::initialize_clerk(socket_addrs));
            clerk.init_once();

            ready.store(true, Ordering::Release);

            while let Ok((request, result)) = rx.recv() {
                let value = match request {
                    ClerkRequest::Get(key) => {
                        clerk.get(key).unwrap_or_default()
                    }
                    ClerkRequest::Put(key, value) => {
                        clerk.put(key, value);
                        String::default()
                    }
                    ClerkRequest::Append(key, value) => {
                        clerk.append(key, value);
                        String::default()
                    }
                };
                let _ = result.send(value);
            }
        });
        return tx;
    }

    fn request(&self, request: ClerkRequest) -> Option<String> {
        if !self.ready.load(Ordering::Acquire) {
            return None;
        }

        let (result_tx, result_rx) = std::sync::mpsc::channel();
        self.requests
            .send((request, result_tx))
            .expect("Send get request should not fail");
        let value = result_rx
            .recv()
            .expect("Receiving get response should not fail");
        Some(value)
    }

    pub(crate) fn get(&self, key: String) -> Option<String> {
        self.request(ClerkRequest::Get(key))
    }

    pub(crate) fn put(&self, key: String, value: String) -> Option<String> {
        self.request(ClerkRequest::Put(key, value))
    }

    pub(crate) fn append(&self, key: String, value: String) -> Option<String> {
        self.request(ClerkRequest::Append(key, value))
    }
}
