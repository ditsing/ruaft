#[macro_export]
macro_rules! log_with {
    ($local_logger:expr, $log:expr) => {{
        let prev = $crate::thread_local_logger::get();
        $crate::thread_local_logger::LocalLogger::attach($local_logger.clone());
        let val = $log;
        $crate::thread_local_logger::set(prev);
        val
    }};
    ($local_logger:expr, $($arg:tt)+) => {{
        $crate::log_with!($local_logger, log::log!($($arg)+));
    }};
}

#[macro_export]
macro_rules! log_trace_with {
    ($local_logger:expr, $($arg:tt)+) => {{
        $crate::log_with!($local_logger, log::trace!($($arg)+));
    }};
}

#[macro_export]
macro_rules! log_debug_with {
    ($local_logger:expr, $($arg:tt)+) => {{
        $crate::log_with!($local_logger, log::debug!($($arg)+));
    }};
}

#[macro_export]
macro_rules! log_info_with {
    ($local_logger:expr, $($arg:tt)+) => {{
        $crate::log_with!($local_logger, log::info!($($arg)+));
    }};
}

#[macro_export]
macro_rules! log_warn_with {
    ($local_logger:expr, $($arg:tt)+) => {{
        $crate::log_with!($local_logger, log::warn!($($arg)+));
    }};
}

#[macro_export]
macro_rules! log_error_with {
    ($local_logger:expr, $($arg:tt)+) => {{
        $crate::log_with!($local_logger, log::error!($($arg)+));
    }};
}

#[cfg(test)]
mod tests {
    use std::panic::catch_unwind;
    use std::sync::{Arc, Mutex, MutexGuard};

    use log::{Level, Log, Metadata, Record};

    use crate::thread_local_logger::LocalLogger;

    #[derive(Clone)]
    struct VecLogs(Arc<Mutex<Vec<(Level, String)>>>);

    impl VecLogs {
        fn entries(&self) -> MutexGuard<Vec<(Level, String)>> {
            self.0.lock().expect("Unlock should never fail")
        }
    }

    impl Log for VecLogs {
        fn enabled(&self, _: &Metadata) -> bool {
            true
        }

        fn log(&self, record: &Record) {
            self.entries()
                .push((record.level(), record.args().to_string()))
        }

        fn flush(&self) {}
    }

    fn setup_logger() -> (LocalLogger, VecLogs) {
        let logs = VecLogs(Arc::new(Mutex::new(vec![])));
        crate::thread_local_logger::thread_init(logs.clone());
        let local_logger = crate::thread_local_logger::get();
        crate::thread_local_logger::reset();
        (local_logger, logs)
    }

    #[test]
    fn test_setup() {
        setup_logger();
        #[cfg(feature = "must-log")]
        catch_unwind(|| log::info!("")).expect_err("Logger should not be set");
    }

    #[test]
    fn test_log_with() {
        let (logger, logs) = setup_logger();
        log_with!(logger, log::info!("Dead beef {}", 1));
        log_with!(logger, log::error!("Dead beef {}", 2));

        assert_eq!(logs.entries().len(), 2);
        assert_eq!(logs.entries()[0], (Level::Info, "Dead beef 1".to_owned()));
        assert_eq!(logs.entries()[1], (Level::Error, "Dead beef 2".to_owned()));
    }

    #[test]
    fn test_log_with_value() {
        let (logger, logs) = setup_logger();
        assert_eq!(log_with!(logger, 1), 1);
        assert_eq!(log_with!(logger, "Dead beef"), "Dead beef");

        assert_eq!(logs.entries().len(), 0);
    }

    #[test]
    fn test_log_with_log() {
        let (logger, logs) = setup_logger();
        log_with!(logger, Level::Warn, "Dead beef {}", 3);
        log_with!(logger, Level::Debug, "Dead beef {}", 4);

        assert_eq!(logs.entries().len(), 2);
        assert_eq!(logs.entries()[0], (Level::Warn, "Dead beef 3".to_owned()));
        assert_eq!(logs.entries()[1], (Level::Debug, "Dead beef 4".to_owned()));
    }

    #[test]
    fn test_log_wrappers() {
        let (logger, logs) = setup_logger();
        log_trace_with!(logger, "Dead beef {}", 5);
        log_debug_with!(logger, "Dead beef {}", 6);
        log_info_with!(logger, "Dead beef {}", 7);
        log_warn_with!(logger, "Dead beef {}", 8);
        log_error_with!(logger, "Dead beef {}", 9);

        assert_eq!(logs.entries().len(), 5);
        assert_eq!(logs.entries()[0], (Level::Trace, "Dead beef 5".to_owned()));
        assert_eq!(logs.entries()[1], (Level::Debug, "Dead beef 6".to_owned()));
        assert_eq!(logs.entries()[2], (Level::Info, "Dead beef 7".to_owned()));
        assert_eq!(logs.entries()[3], (Level::Warn, "Dead beef 8".to_owned()));
        assert_eq!(logs.entries()[4], (Level::Error, "Dead beef 9".to_owned()));
    }
}
