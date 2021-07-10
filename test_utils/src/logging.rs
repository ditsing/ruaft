use std::path::PathBuf;
use std::time::SystemTime;

use rand::Rng;

#[macro_export]
macro_rules! init_test_log {
    () => {
        $crate::init_log(module_path!()).unwrap()
    };
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
    let logger = env_logger::Builder::from_env(env)
        .target(env_logger::Target::Pipe(Box::new(log_file)))
        .filter(Some(module.as_str()), log::LevelFilter::Trace)
        .format_timestamp_millis()
        .is_test(true)
        .build();

    crate::thread_local_logger::thread_init(logger);

    Ok(path)
}
