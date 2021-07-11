/// Test utilities.
/// See [`thread_local_logger`] for more details.
mod logging;
pub mod thread_local_logger;
pub use logging::{init_log, LOG_DIR};
