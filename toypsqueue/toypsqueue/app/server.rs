use toypsqueue;
use std::time::Duration;

pub const DATABASE_FILENAME: &str = "opsqueue.db";

#[tokio::main]
async fn main() {
    println!("Starting Opsqueue");

    let database_filename = DATABASE_FILENAME;
    let producer_server_addr = Box::from("0.0.0.0:3999");
    let consumer_server_addr = Box::from("0.0.0.0:3999");
    let reservation_expiration = Duration::from_secs(60 * 60); // 1 hour

    let db_pool = toypsqueue::db_connect_pool(database_filename).await;

    let consumer_server = toypsqueue::consumer::server::serve(db_pool.clone(), &consumer_server_addr, reservation_expiration);

    let producer_server = toypsqueue::producer::server::serve(db_pool, producer_server_addr);

    let (_, _) = futures::join!(consumer_server, producer_server);

    println!("Stopping Opsqueue");
}
