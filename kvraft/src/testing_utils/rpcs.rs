use common::{GET, PUT_APPEND};
use labrpc::{Network, ReplyMessage, RequestMessage, Server};
use parking_lot::Mutex;
use serde::de::DeserializeOwned;
use serde::Serialize;
use server::KVServer;

fn make_rpc_handler<Request, Reply, F>(
    func: F,
) -> Box<dyn Fn(RequestMessage) -> ReplyMessage>
where
    Request: DeserializeOwned,
    Reply: Serialize,
    F: 'static + Fn(Request) -> Reply,
{
    Box::new(move |request| {
        let reply = func(
            bincode::deserialize(&request)
                .expect("Deserialization should not fail"),
        );

        ReplyMessage::from(
            bincode::serialize(&reply).expect("Serialization should not fail"),
        )
    })
}

pub fn register_kv_server<
    KV: 'static + AsRef<KVServer> + Clone,
    S: AsRef<str>,
>(
    kv: KV,
    name: S,
    network: &Mutex<Network>,
) -> std::io::Result<()> {
    let mut network = network.lock();
    let server_name = name.as_ref();
    let mut server = Server::make_server(server_name);

    let kv_clone = kv.clone();
    server.register_rpc_handler(
        GET.to_owned(),
        make_rpc_handler(move |args| kv_clone.as_ref().get(args)),
    )?;

    let kv_clone = kv.clone();
    server.register_rpc_handler(
        PUT_APPEND.to_owned(),
        make_rpc_handler(move |args| kv_clone.as_ref().put_append(args)),
    )?;

    network.add_server(server_name, server);

    Ok(())
}
