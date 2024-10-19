pub mod conn;
pub mod state;

use std::time::Duration;

pub use conn::ClientConn;
pub use state::ConsumerServerState;
use tokio_util::{sync::CancellationToken, task::TaskTracker};

pub async fn serve(
    database_pool: sqlx::SqlitePool,
    server_addr: Box<str>,
    reservation_expiration: Duration,
    cancellation_token: CancellationToken,
    task_tracker: TaskTracker,
) -> anyhow::Result<()> {
    ConsumerServerState::new(database_pool, reservation_expiration, &server_addr)
        .await
        .run(cancellation_token, task_tracker)
        .await
}
