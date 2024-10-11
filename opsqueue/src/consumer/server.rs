pub mod conn;
pub mod state;

use std::time::Duration;

pub use conn::ClientConn;
pub use state::ConsumerServerState;

pub async fn serve(
    database_pool: sqlx::SqlitePool,
    server_addr: Box<str>,
    reservation_expiration: Duration,
) -> anyhow::Result<()> {
    ConsumerServerState::new(database_pool, reservation_expiration, &server_addr)
        .await
        .run()
        .await
}
