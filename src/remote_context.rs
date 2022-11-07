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
        // Using a pointer to expose a static reference.
        // Using Any to mask the fact that we are storing a generic struct.
        static REMOTE_CONTEXT: RefCell<*mut dyn Any> = RefCell::new(
            std::ptr::null_mut::<()>() as *mut dyn Any);
    }

    pub fn attach(self) {
        Self::set_context(Box::new(self))
    }

    pub fn detach() -> Box<Self> {
        let static_context = Self::fetch_context();
        unsafe { Box::from_raw(static_context) }
    }

    fn set_context(context: Box<Self>) {
        let context_ptr = Box::into_raw(context);
        let any_ptr: *mut dyn Any = context_ptr;
        Self::REMOTE_CONTEXT.with(|context| *context.borrow_mut() = any_ptr);
    }

    fn fetch_context() -> &'static mut Self {
        let any_ptr = Self::REMOTE_CONTEXT.with(|context| *context.borrow());
        if any_ptr.is_null() {
            panic!("Context is not set");
        }
        unsafe {
            (*any_ptr)
                .downcast_mut::<Self>()
                .expect("Context is set to the wrong type.")
        }
    }
}
