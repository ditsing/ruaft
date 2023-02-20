use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};

use tokio::sync::OnceCell;

use crate::common::{
    CommitSentinelArgs, CommitSentinelReply, GetArgs, GetReply, KVError,
    KVRaftOptions, PutAppendArgs, PutAppendEnum, PutAppendReply, UniqueId,
    UniqueIdSequence, ValidReply,
};
use crate::RemoteKvraft;

pub struct AsyncClerk {
    pub inner: AsyncClient,
}

impl AsyncClerk {
    pub fn new(servers: Vec<impl RemoteKvraft>) -> Self {
        Self {
            inner: AsyncClient::new(servers),
        }
    }

    pub async fn get<K: AsRef<str>>(&self, key: K) -> Option<String> {
        self.inner
            .get(key.as_ref().to_owned(), KVRaftOptions::default())
            .await
            .expect("Get should never return error with unlimited retry.")
    }

    pub async fn put<K: AsRef<str>, V: AsRef<str>>(&self, key: K, value: V) {
        let key = key.as_ref();
        let value = value.as_ref();
        self.inner
            .put(key.to_owned(), value.to_owned(), KVRaftOptions::default())
            .await
            .expect("Put should never return error with unlimited retry.")
    }

    pub async fn append<K: AsRef<str>, V: AsRef<str>>(&self, key: K, value: V) {
        let key = key.as_ref();
        let value = value.as_ref();
        self.inner
            .append(key.to_owned(), value.to_owned(), KVRaftOptions::default())
            .await
            .expect("Append should never return error with unlimited retry.")
    }
}

pub struct AsyncClient {
    servers: Vec<Box<dyn RemoteKvraft>>,

    last_server_index: AtomicUsize,
    unique_id: OnceCell<UniqueIdSequence>,
}

impl AsyncClient {
    pub fn new(servers: Vec<impl RemoteKvraft>) -> Self {
        let servers = servers
            .into_iter()
            .map(|s| Box::new(s) as Box<dyn RemoteKvraft>)
            .collect();
        Self {
            servers,

            last_server_index: AtomicUsize::new(0),
            unique_id: OnceCell::new(),
        }
    }

    async fn next_unique_id(&self) -> UniqueId {
        self.unique_id
            .get_or_init(|| self.commit_sentinel())
            .await
            .inc()
    }

    async fn zero_unique_id(&self) -> UniqueId {
        self.unique_id
            .get_or_init(|| self.commit_sentinel())
            .await
            .zero()
    }

    async fn commit_sentinel(&self) -> UniqueIdSequence {
        loop {
            let unique_id = UniqueIdSequence::new();
            let args = CommitSentinelArgs {
                unique_id: unique_id.inc(),
            };
            let reply: Option<CommitSentinelReply> = self
                .retry_rpc(
                    |remote, args| remote.commit_sentinel(args),
                    args,
                    None,
                )
                .await;
            if let Some(reply) = reply {
                match reply.result {
                    Ok(_) => {
                        break unique_id;
                    }
                    Err(KVError::Expired) | Err(KVError::Conflict) => {
                        // The client ID happens to be re-used. The request does
                        // not fail as "Duplicate", because another client has
                        // committed more than just the sentinel.
                        // Do nothing.
                    }
                    Err(e) => {
                        panic!("Unexpected error with indefinite retry: {e:?}");
                    }
                };
            };
        }
    }

    pub async fn retry_rpc<'a, Func, Fut, Args, Reply>(
        &'a self,
        mut future_func: Func,
        args: Args,
        max_retry: Option<usize>,
    ) -> Option<Reply>
    where
        Args: Clone,
        Reply: ValidReply,
        Fut: Future<Output = std::io::Result<Reply>> + Send + 'a,
        Func: FnMut(&'a dyn RemoteKvraft, Args) -> Fut,
    {
        let max_retry =
            std::cmp::max(max_retry.unwrap_or(usize::MAX), self.servers.len());

        let mut index = self.last_server_index.load(Ordering::Relaxed);
        for _ in 0..max_retry {
            let client = &self.servers[index];
            let rpc_response = future_func(client.as_ref(), args.clone()).await;
            if let Ok(ret) = rpc_response {
                if ret.is_reply_valid() {
                    self.last_server_index.store(index, Ordering::Relaxed);
                    return Some(ret);
                }
            }
            index += 1;
            index %= self.servers.len();
        }
        None
    }

    /// This function returns None when
    /// 1. No KVServer can be reached, or
    /// 2. No KVServer claimed to be the leader, or
    /// 3. When the KVServer committed the request but it was not passed
    /// back to the clerk. We must retry with a new unique_id.
    ///
    /// In all 3 cases the request can be retried.
    ///
    /// This function do not expect a Conflict request with the same unique_id.
    pub async fn get(
        &self,
        key: String,
        options: KVRaftOptions,
    ) -> Option<Option<String>> {
        self.zero_unique_id().await;

        let args = GetArgs { key };
        let reply: GetReply = self
            .retry_rpc(|remote, args| remote.get(args), args, options.max_retry)
            .await?;
        match reply.result {
            Ok(val) => Some(val),
            Err(KVError::Expired) => panic!("Get requests do not expire."),
            Err(KVError::Conflict) => panic!("We should never see a conflict."),
            _ => None,
        }
    }

    /// This function returns None when
    /// 1. No KVServer can be reached, or
    /// 2. No KVServer claimed to be the leader.
    ///
    /// Some(()) is returned if the request has been committed previously, under
    /// the assumption is that two different requests with the same unique_id
    /// must be identical.
    ///
    /// This function do not expect a Conflict request with the same unique_id.
    async fn put_append(
        &self,
        key: String,
        value: String,
        op: PutAppendEnum,
        options: KVRaftOptions,
    ) -> Option<()> {
        let unique_id = self.next_unique_id().await;
        let args = PutAppendArgs {
            key,
            value,
            op,
            unique_id,
        };
        let reply: PutAppendReply = self
            .retry_rpc(
                |remote, args| remote.put_append(args),
                args,
                options.max_retry,
            )
            .await?;
        match reply.result {
            Ok(val) => Some(val),
            Err(KVError::Expired) => Some(()),
            Err(KVError::Conflict) => panic!("We should never see a conflict."),
            _ => None,
        }
    }

    pub async fn put(
        &self,
        key: String,
        value: String,
        options: KVRaftOptions,
    ) -> Option<()> {
        self.put_append(key, value, PutAppendEnum::Put, options)
            .await
    }

    pub async fn append(
        &self,
        key: String,
        value: String,
        options: KVRaftOptions,
    ) -> Option<()> {
        self.put_append(key, value, PutAppendEnum::Append, options)
            .await
    }
}

#[cfg(test)]
mod tests {
    use std::collections::hash_map::DefaultHasher;
    use std::collections::HashMap;
    use std::hash::Hasher;
    use std::sync::Arc;

    use async_trait::async_trait;
    use futures::executor::block_on;
    use parking_lot::Mutex;

    use super::*;

    /// A fake server that is only leader when certain requests are received.
    #[derive(Clone)]
    struct FakeRemoteKvraft {
        peer_size: usize,
        id: usize,
        data: Arc<Mutex<HashMap<String, String>>>,
    }

    impl FakeRemoteKvraft {
        fn is_leader(&self, key: &str) -> bool {
            let mut hasher = DefaultHasher::new();
            hasher.write(key.as_bytes());
            let hash_code = hasher.finish();
            (hash_code % (self.peer_size as u64)) as usize == self.id
        }
    }

    #[async_trait]
    impl RemoteKvraft for FakeRemoteKvraft {
        async fn get(&self, args: GetArgs) -> std::io::Result<GetReply> {
            let result = if self.is_leader(&args.key) {
                Ok(self.data.lock().get(&args.key).cloned())
            } else {
                Err(KVError::NotLeader)
            };
            Ok(GetReply { result })
        }

        async fn put_append(
            &self,
            args: PutAppendArgs,
        ) -> std::io::Result<PutAppendReply> {
            assert_ne!(0, args.key.len());
            let result = if self.is_leader(&args.key) {
                let mut data = self.data.lock();
                match args.op {
                    PutAppendEnum::Put => data.insert(args.key, args.value),
                    PutAppendEnum::Append => {
                        let value = args.value + data.get(&args.key).unwrap();
                        data.insert(args.key, value)
                    }
                };
                Ok(())
            } else {
                Err(KVError::NotLeader)
            };
            Ok(PutAppendReply { result })
        }

        async fn commit_sentinel(
            &self,
            args: CommitSentinelArgs,
        ) -> std::io::Result<CommitSentinelReply> {
            let result = if self.is_leader("") {
                let mut data = self.data.lock();
                assert!(!data.contains_key(""));
                let clerk_id = args.unique_id.clerk_id.to_string();
                data.insert("".to_owned(), clerk_id);
                Ok(())
            } else {
                Err(KVError::NotLeader)
            };
            Ok(CommitSentinelReply { result })
        }
    }

    fn create_client() -> AsyncClerk {
        let fake_remote_kvraft0 = FakeRemoteKvraft {
            peer_size: 5,
            id: 0,
            data: Arc::new(Mutex::new(HashMap::new())),
        };
        fake_remote_kvraft0
            .data
            .lock()
            .insert("What clerk?".to_owned(), "async_clerk".to_owned());
        let mut fake_remote_kvraft1 = fake_remote_kvraft0.clone();
        fake_remote_kvraft1.id = 1;
        let mut fake_remote_kvraft2 = fake_remote_kvraft0.clone();
        fake_remote_kvraft2.id = 2;
        let mut fake_remote_kvraft3 = fake_remote_kvraft0.clone();
        fake_remote_kvraft3.id = 3;
        let mut fake_remote_kvraft4 = fake_remote_kvraft0.clone();
        fake_remote_kvraft4.id = 4;

        AsyncClerk::new(vec![
            fake_remote_kvraft0,
            fake_remote_kvraft1,
            fake_remote_kvraft2,
            fake_remote_kvraft3,
            fake_remote_kvraft4,
        ])
    }

    #[test]
    fn test_get_existing_data() {
        let clerk = create_client();
        let existing_data = block_on(clerk.get("What clerk?"));
        assert_eq!(Some("async_clerk".to_owned()), existing_data,);
        let client_id = block_on(clerk.get(""));
        assert!(client_id.is_some());
    }

    #[test]
    fn test_get_put_append() {
        let clerk = create_client();
        block_on(clerk.put("Date", "2017-01-01"));
        block_on(clerk.put("Balance", "97"));

        let date = block_on(clerk.get("Date"));
        assert_eq!(Some("2017-01-01".to_owned()), date);
        let balance = block_on(clerk.get("Balance"));
        assert_eq!(Some("97".to_owned()), balance);

        block_on(clerk.append("Balance", "00"));
        let balance = block_on(clerk.get("Balance"));
        assert_eq!(Some("0097".to_owned()), balance);

        block_on(clerk.put("Balance", "10000"));
        let balance = block_on(clerk.get("Balance"));
        assert_eq!(Some("10000".to_owned()), balance);
    }
}
