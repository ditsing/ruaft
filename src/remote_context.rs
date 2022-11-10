use std::any::Any;
use std::cell::RefCell;

use crate::term_marker::TermMarker;

#[derive(Clone)]
pub(crate) struct RemoteContext<Command> {
    term_marker: TermMarker<Command>,
}

impl<Command: 'static> RemoteContext<Command> {
    pub fn create(term_marker: TermMarker<Command>) -> Self {
        Self { term_marker }
    }

    pub fn term_marker() -> &'static TermMarker<Command> {
        &Self::fetch_context().term_marker
    }

    thread_local! {
        // Using Any to mask the fact that we are storing a generic struct.
        static REMOTE_CONTEXT: RefCell<Option<&'static dyn Any>> = RefCell::new(None);
    }

    pub fn attach(self) {
        Self::set_context(Box::new(self))
    }

    pub fn detach() -> Box<Self> {
        let static_context = Self::fetch_context();
        unsafe { Box::from_raw((static_context as *const Self) as *mut Self) }
    }

    fn set_context(context: Box<Self>) {
        let context_ref = Box::leak(context);
        let any_ref: &'static mut dyn Any = context_ref;
        Self::REMOTE_CONTEXT
            .with(|context| *context.borrow_mut() = Some(any_ref));
    }

    fn fetch_context() -> &'static Self {
        let any_ref = Self::REMOTE_CONTEXT.with(|context| *context.borrow());
        if let Some(any_ref) = any_ref {
            any_ref
                .downcast_ref::<Self>()
                .expect("Context is set to the wrong type.")
        } else {
            panic!("Context is not set");
        }
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use parking_lot::Mutex;
    use std::sync::Arc;

    use crate::election::ElectionState;
    use crate::term_marker::TermMarker;
    use crate::{Peer, Persister, RaftState};

    use super::RemoteContext;

    struct DoNothingPersister;
    impl Persister for DoNothingPersister {
        fn read_state(&self) -> Bytes {
            Bytes::new()
        }

        fn save_state(&self, _bytes: Bytes) {}

        fn state_size(&self) -> usize {
            0
        }

        fn save_snapshot_and_state(&self, _: Bytes, _: &[u8]) {}
    }

    #[test]
    fn test_context_api() {
        let rf = Arc::new(Mutex::new(RaftState::<i32>::create(1, Peer(0))));
        let election = Arc::new(ElectionState::create());
        let term_marker =
            TermMarker::create(rf, election, Arc::new(DoNothingPersister));

        let context = Box::new(RemoteContext::create(term_marker));
        let context_ptr: *const RemoteContext<i32> = &*context;

        RemoteContext::set_context(context);

        let fetched_context = RemoteContext::fetch_context();
        let fetched_context_ptr: *const RemoteContext<i32> = fetched_context;
        assert_eq!(context_ptr, fetched_context_ptr);

        let detached_context = RemoteContext::detach();
        let detached_context_ptr: *const RemoteContext<i32> =
            &*detached_context;
        assert_eq!(context_ptr, detached_context_ptr);
    }
}
