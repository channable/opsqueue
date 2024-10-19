use opsqueue;
use std::time::Duration;

pub const DATABASE_FILENAME: &str = "opsqueue.db";

#[tokio::main]
async fn main() {
    println!("Starting Opsqueue");

    let subscriber = tracing_subscriber();
    tracing::subscriber::set_global_default(subscriber).expect("Error setting up global tracing subscriber");

    tracing::info!("Finished setting up tracing subscriber");


    let database_filename = DATABASE_FILENAME;

    opsqueue::ensure_db_exists(database_filename).await;
    let db_pool = opsqueue::db_connect_pool(database_filename).await;
    opsqueue::ensure_db_migrated(&db_pool).await;

    let producer_server_addr = Box::from("0.0.0.0:3999");
    let consumer_server_addr = Box::from("0.0.0.0:3998");
    let reservation_expiration = Duration::from_secs(60 * 60); // 1 hour

    let consumer_server = opsqueue::consumer::server::serve(
        db_pool.clone(),
        consumer_server_addr,
        reservation_expiration,
    );
    let producer_server = opsqueue::producer::server::serve(db_pool, producer_server_addr);

    tokio::spawn(async move { consumer_server.await });
    tokio::spawn(async move { producer_server.await });

    tokio::signal::ctrl_c()
        .await
        .expect("Failed to set up Ctrl+C signal handler");

    tracing::warn!("Opsqueue is shutting down");
    println!("");
    println!("Opsqueue Stopped");
}

/// Sets up the global tracing subscriber.
/// Current choices are based on the defaults described
/// in the Tokio tracing tutorial https://tokio.rs/tokio/topics/tracing
fn tracing_subscriber() -> impl tracing::Subscriber {
    // Start configuring a `fmt` subscriber
    tracing_subscriber::fmt()
    // The log level is set based on the `RUST_LOG` environment variable.
    // ex: `RUST_LOG="debug"` shows all error, warn, info and debug logs (but no trace logs) across all crates.
    // `RUST_LOG="opsqueue=trace"` shows _all_ logs for opsqueue, including trace logs.
    .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
    // Use a more compact, abbreviated log format
    // .compact()
    // Display source code file paths
    .with_file(true)
    // Display source code line numbers
    .with_line_number(true)
    // Display the thread ID an event was recorded on
    .with_thread_ids(true)
    // Don't display the event's target (module path)
    .with_target(false)
    // Build the subscriber
    .finish()
}
