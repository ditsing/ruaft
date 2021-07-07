extern crate env_logger;
extern crate log;
extern crate rand;

mod logging;
pub use logging::{init_log, init_log_once, LOG_DIR};
