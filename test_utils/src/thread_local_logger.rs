use std::cell::RefCell;
use std::sync::{Arc, Once};

use log::{Log, Metadata, Record};
use std::fmt::Formatter;
use std::ops::Deref;

pub struct GlobalLogger;
#[cfg(not(feature = "must-log"))]
#[derive(Clone)]
pub struct LocalLogger(Arc<dyn Log>);
#[cfg(feature = "must-log")]
#[derive(Clone)]
pub struct LocalLogger(Option<Arc<dyn Log>>);

thread_local!(static LOCAL_LOGGER: RefCell<LocalLogger> = Default::default());

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

pub fn thread_init<T: 'static + Log>(logger: T) {
    global_init_once();
    #[cfg(not(feature = "must-log"))]
    self::set(LocalLogger(Arc::new(logger)));
    #[cfg(feature = "must-log")]
    self::set(LocalLogger(Some(Arc::new(logger))));
}

pub fn get() -> LocalLogger {
    let result = LOCAL_LOGGER.with(|inner| inner.borrow().clone());
    let _ = result.deref();
    result
}

pub fn set(logger: LocalLogger) {
    LOCAL_LOGGER.with(|inner| inner.replace(logger));
}

pub fn reset() {
    self::set(LocalLogger::default())
}

impl LocalLogger {
    pub fn inherit() -> Self {
        get()
    }

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
