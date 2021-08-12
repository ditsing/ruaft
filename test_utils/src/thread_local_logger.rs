/// A logger that is local to each thread.
///
/// Logger as defined by the `log` crate is a global construct. A logger is
/// shared by the whole process and all of its threads. This makes sense for an
/// application.
///
/// However, a global logger is rarely useful for tests. `cargo test` creates
/// one process for each test source file, and runs each test method in a
/// separate thread. By default, logs of different tests will be mixed together,
/// which is less useful.
///
/// A similar problem exists in tests for `stdout` and `stderr`. Therefore,
/// `libtest` implemented something called `output capture`. Internally in crate
/// `std::io`, output `stdout` and `stderr` of a thread can be configured to
/// redirected to a sink of type `Arc<Mutex<Vec<u8>>>`. The sink is stored in a
/// `thread_local` variable. For each test method, `libtest` creates a sink
/// specific to the test and stores it in the main test thread. The output of
/// the main test thread will be redirected to the sink by `std::io`. Later in
/// `std::thread::spawn()`, when a new thread is created, the sink is copied to
/// the new thread's local variable. Thus, the output of the new thread is also
/// redirected to the same sink. That is how tests output are captured.
///
/// We took a similar approach by storing a logger in each thread, and copy the
/// logger to all sub-threads. Unfortunately we do not have access to methods
/// like `std::thread::spawn()`, thus the copying can only be done manually.
use std::cell::RefCell;
use std::sync::{Arc, Once};

use log::{Log, Metadata, Record};
use std::fmt::Formatter;
use std::ops::Deref;

struct GlobalLogger;
#[cfg(not(feature = "must-log"))]
#[derive(Clone)]
pub struct LocalLogger(Arc<dyn Log>);
#[cfg(feature = "must-log")]
#[derive(Clone)]
pub struct LocalLogger(Option<Arc<dyn Log>>);

thread_local!(static LOCAL_LOGGER: RefCell<LocalLogger> = Default::default());

/// Initialized the global logger used by the `log` crate.
///
/// This method can be called multiple times by different tests. The logger is
/// only initialized once and it works for all tests and all threads.
pub fn global_init_once() {
    static GLOBAL_LOGGER: GlobalLogger = GlobalLogger;
    static INIT_LOG_ONCE: Once = Once::new();

    INIT_LOG_ONCE.call_once(|| {
        log::set_logger(&GLOBAL_LOGGER)
            .expect("Set global logger should never fail");
        // Enable all logging globally. Each local logger should have its own
        // filtering, which could be less efficient.
        log::set_max_level(log::LevelFilter::max());
    });
}

/// Set the logger for the main test thread.
///
/// This method should only be called once at the beginning of each test.
///
/// Before creating a child thread, use [`LocalLogger::inherit()`] to copy the
/// logger from the parent thread. Move the copied logger to the child thread
/// and use [`LocalLogger::attach()`] to set the logger for the child thread.
pub fn thread_init<T: 'static + Log>(logger: T) {
    global_init_once();
    #[cfg(not(feature = "must-log"))]
    self::set(LocalLogger(Arc::new(logger)));
    #[cfg(feature = "must-log")]
    self::set(LocalLogger(Some(Arc::new(logger))));
}

#[doc(hidden)]
pub fn get() -> LocalLogger {
    LOCAL_LOGGER.with(|inner| inner.borrow().clone())
}

#[doc(hidden)]
pub fn set(logger: LocalLogger) {
    LOCAL_LOGGER.with(|inner| inner.replace(logger));
}

#[doc(hidden)]
pub fn reset() {
    self::set(LocalLogger::default())
}

impl LocalLogger {
    /// Inherit the logger from the current thread.
    pub fn inherit() -> Self {
        let result = get();
        let _ = result.deref();
        result
    }

    /// Set the logger of this thread to `self`.
    pub fn attach(self) {
        set(self)
    }
}

impl Default for LocalLogger {
    fn default() -> Self {
        #[cfg(not(feature = "must-log"))]
        {
            Self(Arc::new(NopLogger))
        }
        #[cfg(feature = "must-log")]
        Self(None)
    }
}

impl std::ops::Deref for LocalLogger {
    type Target = dyn Log;

    fn deref(&self) -> &Self::Target {
        #[cfg(not(feature = "must-log"))]
        {
            self.0.deref()
        }
        #[cfg(feature = "must-log")]
        self.0
            .as_ref()
            .expect("Local logger must be set before use")
            .as_ref()
    }
}

impl std::fmt::Debug for LocalLogger {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("thread local logger")
    }
}

impl Log for GlobalLogger {
    fn enabled(&self, metadata: &Metadata) -> bool {
        LOCAL_LOGGER.with(|inner| inner.borrow().enabled(metadata))
    }

    fn log(&self, record: &Record) {
        LOCAL_LOGGER.with(|inner| inner.borrow().log(record))
    }

    fn flush(&self) {
        LOCAL_LOGGER.with(|inner| inner.borrow().flush())
    }
}

struct NopLogger;

impl Log for NopLogger {
    fn enabled(&self, _: &Metadata) -> bool {
        false
    }

    fn log(&self, _: &Record) {}
    fn flush(&self) {}
}
