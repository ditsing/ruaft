extern crate env_logger;
extern crate log;
extern crate rand;

mod logging;
pub mod thread_local_logger;
pub use logging::{init_log, LOG_DIR};
