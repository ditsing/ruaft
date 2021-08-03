use labrpc::{Network, Server};
use parking_lot::Mutex;

use test_configs::make_rpc_handler;

use crate::common::{GET, PUT_APPEND};
use crate::server::KVServer;

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

    server.register_rpc_handler(
        PUT_APPEND.to_owned(),
        make_rpc_handler(move |args| kv.as_ref().put_append(args)),
    )?;

    network.add_server(server_name, server);

    Ok(())
}
