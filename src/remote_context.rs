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
