/// A `std::sync::mpsc::Sender` that is also `Sync`.
///
/// The builtin `Sender` is not sync, because it uses internal mutability to
/// implement an optimization for non-shared one-shot sending. The queue that
/// backs the sender initially accepts only one item from a single producer.
/// If the sender is cloned, the internal queue turns into a multi-producer
/// multi-shot queue. After that, the internal mutability is never invoked
/// again for the sender. The `Sender` structure becomes essentially immutable
/// and thus, `Sync`.
///
/// This optimization, and the internal mutability is meaningless for the
/// purpose of this crate. `SharedSender` forces the transition into a shared
/// queue, and declares itself `Sync`.
///
/// Note that the same reasoning does not apply to the `Receiver`. There are
/// more levels of mutability in the `Receiver`.
#[derive(Debug)]
pub struct SharedSender<T>(std::sync::mpsc::Sender<T>);

unsafe impl<T> Sync for SharedSender<T> where T: Sync {}
// A better way to implement this might be the following.
//
// unsafe impl<T> Sync for SharedSender<T> where
//    std::sync::mpsc::Flavor<T>::Shared: Sync {}

impl<T> SharedSender<T> {
    /// Create a shared sender.
    pub fn new(inner: std::sync::mpsc::Sender<T>) -> SharedSender<T> {
        // Force the transition to a shared queue in Sender.
        let _clone = inner.clone();
        SharedSender(inner)
    }

    /// A proxy to `std::syc::mpsc::Sender::send()`.
    pub fn send(&self, t: T) -> Result<(), std::sync::mpsc::SendError<T>> {
        self.0.send(t)
    }
}

impl<T> From<std::sync::mpsc::Sender<T>> for SharedSender<T> {
    fn from(inner: std::sync::mpsc::Sender<T>) -> Self {
        Self::new(inner)
    }
}

impl<T> From<SharedSender<T>> for std::sync::mpsc::Sender<T> {
    fn from(this: SharedSender<T>) -> Self {
        this.0
    }
}

impl<T> Clone for SharedSender<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}
