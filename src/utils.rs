use std::future::Future;
use std::time::Duration;

pub async fn retry_rpc<Func, Fut, T>(
    max_retry: usize,
    mut task_gen: Func,
) -> std::io::Result<T>
where
    Fut: Future<Output = std::io::Result<T>> + Send + 'static,
    Func: FnMut(usize) -> Fut,
{
    for i in 0..max_retry {
        if let Ok(reply) = task_gen(i).await {
            return Ok(reply);
        }
        tokio::time::delay_for(Duration::from_millis((1 << i) * 10)).await;
    }
    Err(std::io::Error::new(
        std::io::ErrorKind::TimedOut,
        format!("Timed out after {} retries", max_retry),
    ))
}

pub struct DropGuard<F: FnOnce()> {
    task: Option<F>,
}

impl<F: FnOnce()> DropGuard<F> {
    pub fn new(task: F) -> Self {
        Self { task: Some(task) }
    }
}

impl<F: FnOnce()> Drop for DropGuard<F> {
    fn drop(&mut self) {
        (self.task.take().unwrap())();
    }
}
