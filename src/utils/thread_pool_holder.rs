lazy_static::lazy_static! {
    static ref THREAD_POOLS: parking_lot::Mutex<std::collections::HashMap<u64, tokio::runtime::Runtime>> =
        parking_lot::Mutex::new(std::collections::HashMap::new());
}

#[derive(Clone)]
pub struct ThreadPoolHolder {
    id: u64,
    handle: tokio::runtime::Handle,
}

impl ThreadPoolHolder {
    pub fn new(runtime: tokio::runtime::Runtime) -> Self {
        let handle = runtime.handle().clone();
        loop {
            let id: u64 = rand::random();
            if let std::collections::hash_map::Entry::Vacant(v) =
                THREAD_POOLS.lock().entry(id)
            {
                v.insert(runtime);
                break Self { id, handle };
            }
        }
    }

    pub fn take(self) -> Option<tokio::runtime::Runtime> {
        THREAD_POOLS.lock().remove(&self.id)
    }
}

impl std::ops::Deref for ThreadPoolHolder {
    type Target = tokio::runtime::Handle;

    fn deref(&self) -> &Self::Target {
        &self.handle
    }
}
