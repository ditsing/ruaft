use std::cell::RefCell;
use std::sync::{Arc, Once};

use log::{Log, Metadata, Record};
use std::fmt::Formatter;

pub struct GlobalLogger;
#[derive(Clone)]
pub struct LocalLogger(Arc<dyn Log>);

thread_local!(static LOCAL_LOGGER: RefCell<LocalLogger> = Default::default());

pub fn global_init_once() {
    static GLOBAL_LOGGER: GlobalLogger = GlobalLogger;
    static INIT_LOG_ONCE: Once = Once::new();

    INIT_LOG_ONCE.call_once(|| {
        log::set_logger(&GLOBAL_LOGGER).unwrap();
        // Enable all logging globally. Each local logger should have its own
        // filtering, which could be less efficient.
        log::set_max_level(log::LevelFilter::max());
    });
}

pub fn thread_init<T: 'static + Log>(logger: T) {
    global_init_once();
    self::set(LocalLogger(Arc::new(logger)));
}

pub fn get() -> LocalLogger {
    LOCAL_LOGGER.with(|inner| inner.borrow().clone())
}

pub fn set(logger: LocalLogger) {
    LOCAL_LOGGER.with(|inner| inner.replace(logger));
}

pub fn reset() {
    self::set(LocalLogger::default())
}

impl Default for LocalLogger {
    fn default() -> Self {
        Self(Arc::new(NopLogger))
    }
}

impl std::ops::Deref for LocalLogger {
    type Target = dyn Log;

    fn deref(&self) -> &Self::Target {
        self.0.deref()
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
