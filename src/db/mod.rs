mod model_notification;
mod model_task;

pub mod scylladb;

pub use model_notification::{GroupNotification, Notification};
pub use model_task::Task;
