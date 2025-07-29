//! Defines the source of truth for configuring the Opsqueue queue
//!
//! We make use of the excellent `clap` crate to make customizing the configuration
//! with command-line args easier.
use std::num::NonZero;

use clap::Parser;

/// Making big work horizontally scalable.
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
pub struct Config {
    /// TCP port to bind the HTTP server to.
    ///
    /// This port will be used both for the producer and consumer APIs,
    /// as well as the `/metrics` and `/ping` endpoints.
    #[arg(short, long, default_value_t = 3999)]
    pub port: u16,

    /// Name of the SQLite database file used by this opsqueue.
    ///
    /// Configure this to different values when you run multiple opsqueues
    /// for different purposes.
    #[arg(long, default_value = "opsqueue.db")]
    pub database_filename: String,

    /// Maximum duration a consumer is allowed to take per chunk.
    ///
    /// After this time has expired, the consumer will receive a signal to drop the chunk,
    /// and the chunk will be available for other consumers.
    ///
    /// This setting exists as an extra security measure to
    /// stop 'bad' chunks from making consumers hang indefinitely:
    ///
    /// - ensuring that a consumer pool as a whole doesn't become unresponsive;
    ///
    /// - allowing developers to eagerly identify these 'bad' chunks
    ///   and fix their consumer implementations.
    ///
    #[arg(long, default_value = "10 minutes")]
    pub reservation_expiration: humantime::Duration,

    /// Maximum number of SQLite connections to keep in memory for reading.
    /// Connections will only be opened when needed.
    ///
    /// For the best performance, keep this number as high
    /// as the expected maximum number of concurrent consumers.
    ///
    /// Note that each connection requires a file descriptor, so ensure `ulimit -n` is sufficiently high.
    #[arg(long, default_value_t = NonZero::new(256).unwrap())]
    pub max_read_pool_size: NonZero<u32>,

    /// Maximum duration between
    /// messages on the opsqueue<->consumer persistent connection
    /// before a special heartbeat message is sent.
    ///
    /// Note that special heartbeat messages are only sent if
    /// there was no other message sent/received within the given interval;
    /// any message sent/received on the connection will reset the interval
    /// as well as the 'missed_heartbeats' counter.
    ///
    /// It is recommended to keep this value in the seconds range.
    #[arg(long, default_value = "10 seconds")]
    pub heartbeat_interval: humantime::Duration,

    /// Maximum number of missed heartbeats allowed before
    /// considering the opsqueue<->consumer persistent connection to be unhealthy.
    ///
    /// At that time, the connection will be closed and any open reservations will be canceled.
    #[arg(long, default_value_t = 3)]
    pub max_missable_heartbeats: usize,
}

impl Default for Config {
    fn default() -> Self {
        use std::str::FromStr;
        let port = 3999;
        let database_filename = "opsqueue.db".to_string();
        let reservation_expiration =
            humantime::Duration::from_str("10 minutes").expect("valid humantime");
        let max_read_pool_size = NonZero::new(256).unwrap();
        let heartbeat_interval =
            humantime::Duration::from_str("10 seconds").expect("valid humantime");
        let max_missable_heartbeats = 3;
        Config {
            port,
            database_filename,
            reservation_expiration,
            max_read_pool_size,
            heartbeat_interval,
            max_missable_heartbeats,
        }
    }
}
