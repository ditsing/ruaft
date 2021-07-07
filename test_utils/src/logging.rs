use std::path::PathBuf;
use std::time::SystemTime;

use rand::Rng;
use std::sync::Once;

#[macro_export]
macro_rules! init_test_log {
    () => {
        $crate::init_log_once(module_path!())
    };
}

static INIT_LOG_ONCE: Once = Once::new();
static mut LOG_FILE: Option<PathBuf> = None;

pub fn init_log_once(module_path: &str) -> PathBuf {
    INIT_LOG_ONCE.call_once(|| unsafe {
        LOG_FILE.replace(
            init_log(module_path).expect("init_log() should never fail"),
        );
    });
    unsafe { LOG_FILE.clone().expect("The log file should have been set") }
}

pub const LOG_DIR: &str = "/tmp/ruaft-test-logs/";

pub fn init_log(module_path: &str) -> std::io::Result<PathBuf> {
    let module = module_path.replace("::config", "");
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
    let log_file_name =
        format!("{}-{:010}-{}.log", module_file, timestamp, suffix);

    let log_dir = option_env!("LOG_DIR").unwrap_or(LOG_DIR);
    let mut path = PathBuf::from(log_dir);
    std::fs::create_dir_all(path.as_path())?;

    path.push(log_file_name);
    let log_file = std::fs::File::create(path.as_path())?;

    let env = env_logger::Env::default().default_filter_or("info");
    env_logger::Builder::from_env(env)
        .target(env_logger::Target::Pipe(Box::new(log_file)))
        .filter(Some(module.as_str()), log::LevelFilter::Trace)
        .format_timestamp_millis()
        .is_test(true)
        .init();

    Ok(path)
}
