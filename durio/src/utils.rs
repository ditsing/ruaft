use std::io::ErrorKind;
use std::net::SocketAddr;

use futures_util::StreamExt;
use tarpc::client::RpcError;
use tarpc::server::{Channel, Serve};

pub(crate) fn deadline_forever() -> std::time::SystemTime {
    std::time::SystemTime::now()
        // This is the maximum deadline allowed by tarpc / tokio_util.
        + std::time::Duration::from_secs(2 * 365 * 24 * 60 * 60)
}

pub(crate) fn context() -> tarpc::context::Context {
    let mut context = tarpc::context::Context::current();
    context.deadline = deadline_forever();
    context
}

pub(crate) fn translate_rpc_error(e: RpcError) -> std::io::Error {
    match e {
        RpcError::Disconnected => std::io::Error::new(ErrorKind::BrokenPipe, e),
        RpcError::DeadlineExceeded => {
            std::io::Error::new(ErrorKind::TimedOut, e)
        }
        RpcError::Server(server_error) => {
            std::io::Error::new(ErrorKind::Other, server_error)
        }
    }
}

pub(crate) async fn start_tarpc_server<Request, Reply, ServeFn>(
    addr: SocketAddr,
    serve: ServeFn,
) -> std::io::Result<()>
where
    Request: Send + 'static + serde::de::DeserializeOwned,
    Reply: Send + 'static + serde::ser::Serialize,
    ServeFn:
        tarpc::server::Serve<Request, Resp = Reply> + Send + 'static + Clone,
    <ServeFn as Serve<Request>>::Fut: Send,
{
    let mut listener = tarpc::serde_transport::tcp::listen(
        addr,
        tokio_serde::formats::Json::default,
    )
    .await?;

    tokio::spawn(async move {
        while let Some(conn) = listener.next().await {
            if let Ok(conn) = conn {
                let channel = tarpc::server::BaseChannel::with_defaults(conn)
                    .max_concurrent_requests(1);
                tokio::spawn(channel.execute(serve.clone()));
            }
        }
    });
    Ok(())
}
