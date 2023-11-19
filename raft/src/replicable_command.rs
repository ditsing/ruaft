/// A command must satisfy the requirement of this trait, so that it could be
/// replicated in Raft.
// Commands must be
// 0. 'static: they have to live long enough for thread pools.
// 1. clone: they are put in vectors and request messages.
// 2. serializable: they are sent over RPCs and persisted.
// 3. deserializable: they are restored from storage.
// 4. send: they are referenced in futures.
pub trait ReplicableCommand:
    'static + Clone + serde::Serialize + serde::de::DeserializeOwned + Send
{
}

impl<C> ReplicableCommand for C where
    C: 'static + Clone + serde::Serialize + serde::de::DeserializeOwned + Send
{
}
