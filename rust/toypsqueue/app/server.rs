pub const DATABASE_FILENAME: &str = "opsqueue.db";

#[tokio::main]
async fn main() {
    let database_filename = DATABASE_FILENAME;
    let server_addr = "0.0.0.0:3999";

    toypsqueue::server::serve(database_filename, server_addr).await;
}
