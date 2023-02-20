use std::path::PathBuf;
use std::time::SystemTime;

use rand::Rng;

/// Initialize a thread local logger for a test.
///
/// A log file will be created in [`LOG_DIR`]. The name of the log file will
/// be derived from the fully qualified test method name, plus a timestamp and
/// 10 random characters, e.g. `mod-test_method-1600000000-abcdefghij.log`.
///
/// The test logger is backed by `env_logger`. By default the test module will
/// have `trace` level logging. Other modules have `info` level logging. See the
/// documentation of `env_logger` on how to configure log levels.
#[macro_export]
macro_rules! init_test_log {
    () => {
        $crate::init_log(stdext::function_name!())
            .expect("Initializing test log should never fail")
    };
}

pub const LOG_DIR: &str = "/tmp/ruaft-test-logs/";

/// Initialize a thread local logger for `module`.
///
/// See [`init_test_log`] for more details.
pub fn init_log(module: &str) -> std::io::Result<PathBuf> {
    let module_file = module.replace("::", "-");
    let timestamp = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let suffix: String = rand::thread_rng()
        .sample_iter(&rand::distributions::Alphanumeric)
        .take(10)
        .map(char::from)
        .collect();
    let log_file_name = format!("{module_file}-{timestamp:010}-{suffix}.log");

    let log_dir = option_env!("LOG_DIR").unwrap_or(LOG_DIR);
    let mut path = PathBuf::from(log_dir);
    std::fs::create_dir_all(path.as_path())?;

    path.push(log_file_name);
    let log_file = std::fs::File::create(path.as_path())?;

    {
        let mut latest_path = path.clone();
        latest_path.pop();
        latest_path.push(format!("{module_file}-latest"));
        let _ = std::fs::remove_file(latest_path.as_path());
        #[cfg(unix)]
        let _ = std::os::unix::fs::symlink(path.as_path(), latest_path);
        #[cfg(windows)]
        let _ = std::os::windows::fs::symlink_file(path.as_path(), latest_path);
    }

    let module = match module.rfind("::") {
        Some(pos) => &module[..pos],
        None => module,
    };

    let env = env_logger::Env::default().default_filter_or("info");
    let logger = env_logger::Builder::from_env(env)
        .target(env_logger::Target::Pipe(Box::new(log_file)))
        .filter(Some(module), log::LevelFilter::Trace)
        .format_timestamp_millis()
        .is_test(true)
        .build();

    crate::thread_local_logger::thread_init(logger);

    Ok(path)
}
