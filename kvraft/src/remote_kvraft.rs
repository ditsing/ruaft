use async_trait::async_trait;

#[async_trait]
pub trait RemoteKvraft: Send + Sync + 'static {
    async fn call_rpc(
        &self,
        method: String,
        request: Vec<u8>,
    ) -> std::io::Result<Vec<u8>>;
}
