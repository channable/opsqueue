use toypsqueue::{self, ensure_db_exists};
use std::time::Duration;
use sqlx::migrate::Migrator;

pub const DATABASE_FILENAME: &str = "opsqueue.db";

#[tokio::main]
async fn main() {
    println!("Starting Opsqueue");

    let database_filename = DATABASE_FILENAME;

    toypsqueue::ensure_db_exists(database_filename).await;
    let db_pool = toypsqueue::db_connect_pool(database_filename).await;
    toypsqueue::ensure_db_migrated(&db_pool).await;


    let producer_server_addr = Box::from("0.0.0.0:3999");
    let consumer_server_addr = Box::from("0.0.0.0:3998");
    let reservation_expiration = Duration::from_secs(60 * 60); // 1 hour


    let consumer_server = toypsqueue::consumer::server::serve(db_pool.clone(), consumer_server_addr, reservation_expiration);
    let producer_server = toypsqueue::producer::server::serve(db_pool, producer_server_addr);

    tokio::spawn(async move { consumer_server.await });
    tokio::spawn(async move { producer_server.await });

    tokio::signal::ctrl_c().await.expect("Failed to set up Ctrl+C signal handler");

    println!("");
    println!("Stopping Opsqueue");
}
