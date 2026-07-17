//! Defines the source of truth for configuring the Opsqueue queue
//!
//! We make use of the excellent `clap` crate to make customizing the configuration
//! with command-line args easier.
use std::{
    fs::File,
    io::{self, Write},
    num::NonZero,
    os::fd::FromRawFd,
    sync::{Arc, Mutex},
};

use clap::Parser;

use crate::common::MaxSubmissions;

fn default_max_submissions_returned() -> MaxSubmissions {
    MaxSubmissions::new(NonZero::new(100_000).expect("Non-zero u64"))
        .expect("Valid MaxSubmissions default")
}

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

    /// File descriptor where the final bound TCP port is written.
    ///
    /// This is useful when `--port 0` is used and a parent process wants to receive the assigned
    /// port without filesystem IO.
    ///
    /// The port is written as a 16-bit (u16) big-endian integer to the given file descriptor which
    /// is then closed. On error the file descriptor is closed without writing anything.
    #[arg(long, value_parser = parse_report_bound_port_fd, default_value = "-1")]
    pub report_bound_port_pipe: ReportBoundPortPipe,

    /// Name of the `SQLite` database file used by this opsqueue.
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

    /// Maximum number of `SQLite` connections to keep in memory for reading.
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
    /// as well as the '`missed_heartbeats`' counter.
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

    /// Maximum number of times a chunk is retried before permanently failing
    /// the full submission the chunk is a part of.
    #[arg(long, default_value_t = 10)]
    pub max_chunk_retries: u32,

    #[arg(long, default_value = "1 hour")]
    pub max_submission_age: humantime::Duration,

    /// Maximum number of submission IDs that a single
    /// `lookup_submission_ids_by_strategic_metadata` request may return.
    #[arg(long, default_value_t = default_max_submissions_returned())]
    pub max_submissions_returned: MaxSubmissions,
}

impl Default for Config {
    fn default() -> Self {
        use std::str::FromStr;
        let port = 3999;
        let report_bound_port_pipe = ReportBoundPortPipe::default();
        let database_filename = "opsqueue.db".to_string();
        let reservation_expiration =
            humantime::Duration::from_str("10 minutes").expect("valid humantime");
        let max_read_pool_size = NonZero::new(256).unwrap();
        let heartbeat_interval =
            humantime::Duration::from_str("10 seconds").expect("valid humantime");
        let max_missable_heartbeats = 3;
        let max_chunk_retries = 10;
        let max_submission_age = humantime::Duration::from_str("1 hour").expect("valid humantime");
        let max_submissions_returned = default_max_submissions_returned();
        Config {
            port,
            report_bound_port_pipe,
            database_filename,
            reservation_expiration,
            max_read_pool_size,
            heartbeat_interval,
            max_missable_heartbeats,
            max_chunk_retries,
            max_submission_age,
            max_submissions_returned,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct ReportBoundPortPipe(Arc<Mutex<Option<BoundPortPipe>>>);

impl ReportBoundPortPipe {
    #[must_use]
    pub fn take(&self) -> Option<BoundPortPipe> {
        self.0.lock().expect("No poison").take()
    }
}

#[derive(Debug)]
pub struct BoundPortPipe(File);

impl BoundPortPipe {
    pub fn write_port(mut self, port: u16) -> io::Result<()> {
        self.0.write_all(&u16::to_be_bytes(port))?;
        self.0.flush()
    }
}

fn parse_report_bound_port_fd(value: &str) -> Result<ReportBoundPortPipe, String> {
    let fd = value
        .parse::<i32>()
        .map_err(|err| format!("invalid file descriptor {value:?}: {err}"))?;

    if fd == -1 {
        return Ok(ReportBoundPortPipe::default());
    }

    if fd < 0 {
        return Err(format!(
            "invalid file descriptor {value:?}: must be non-negative"
        ));
    }

    // SAFETY: the parent process passes ownership of this FD to us through `pass_fds`.
    Ok(ReportBoundPortPipe(Arc::new(Mutex::new(Some(
        BoundPortPipe(unsafe { File::from_raw_fd(fd) }),
    )))))
}
