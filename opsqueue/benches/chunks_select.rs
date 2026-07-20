//! Benchmarks the chunk-selection query cost, with no network in the path.
//!
//! It seeds a SQLite database with a backlog and times how long the selection query
//! takes to return the first chunk (i.e. reserving one chunk). Doing this in Rust
//! avoids the ~40ms per-request round-trip floor of the Python integration benchmark,
//! which otherwise hides the query cost entirely.
//!
//! Two backlog shapes are swept over increasing size:
//!   * `few_submissions_many_chunks`: PreferDistinct ranks few submissions, so the query
//!     stays flat rather than walking the whole `chunks` table.
//!   * `many_submissions_few_chunks`: PreferDistinct must rank every submission, so its
//!     cost grows with the backlog.
//!
//! Results are written to `target/chunks_select_bench.csv`
//! (`shape,strategy,backlog_size,median_us`); plot them with `plot_chunks_select.py`.

use std::io::Write;
use std::num::NonZero;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use futures::stream::TryStreamExt as _;

use opsqueue::common::StrategicMetadataMap;
use opsqueue::common::chunk::{Chunk, ChunkSize};
use opsqueue::common::submission::db::insert_submission_from_chunks;
use opsqueue::consumer::dispatcher::metastate::MetaState;
use opsqueue::consumer::strategy::Strategy;
use opsqueue::db::{self, Connection as _};

const BACKLOG_SIZES: &[usize] = &[100, 300, 1_000, 3_000, 10_000, 30_000];
const SAMPLES: usize = 30;
const WARMUP: usize = 5;
/// A reserver only ever holds a bounded number of chunks in flight, so we count at most
/// this many companies in the MetaState (keeping the ranking's `counts` map realistic).
const MAX_IN_FLIGHT: usize = 256;

static DB_COUNTER: AtomicU64 = AtomicU64::new(0);

fn strategies() -> Vec<(&'static str, Strategy)> {
    vec![
        ("Random", Strategy::Random),
        (
            "PreferDistinct(company_id, Oldest)",
            Strategy::PreferDistinct {
                meta_key: "company_id".to_string(),
                underlying: Box::new(Strategy::Oldest),
            },
        ),
    ]
}

/// (submissions, chunks per submission) for a shape given a total chunk budget.
fn layout(shape: &str, total_chunks: usize) -> (usize, usize) {
    match shape {
        "many_submissions_few_chunks" => (total_chunks, 1),
        "few_submissions_many_chunks" => {
            let submissions = 10.min(total_chunks.max(1));
            (submissions, total_chunks.div_ceil(submissions))
        }
        other => panic!("unknown shape: {other}"),
    }
}

/// Seed a fresh on-disk DB with a backlog of the given shape, returning the pools, a
/// MetaState in which up to `MAX_IN_FLIGHT` companies have a few chunks "in flight" so
/// the ranking has work to do, and the temp DB path (delete it when done).
async fn seed(shape: &str, total_chunks: usize) -> (db::DBPools, MetaState, std::path::PathBuf) {
    let (submissions, chunks_per_submission) = layout(shape, total_chunks);
    let unique = DB_COUNTER.fetch_add(1, Ordering::Relaxed);
    let path = std::env::temp_dir().join(format!("opsqueue_bench_{unique}.sqlite"));
    let _ = std::fs::remove_file(&path);
    let db_pools =
        db::open_and_setup(path.to_str().unwrap(), NonZero::new(16).unwrap()).await;
    let mut conn = db_pools.writer_conn().await.unwrap();
    let metastate = MetaState::default();
    for company in 0..submissions as i64 {
        let mut metadata = StrategicMetadataMap::default();
        metadata.insert("company_id".to_string(), company);
        let chunks = vec![Some(b"x".to_vec()); chunks_per_submission];
        insert_submission_from_chunks(None, chunks, None, metadata, ChunkSize::default(), &mut conn)
            .await
            .unwrap();
        if (company as usize) < MAX_IN_FLIGHT {
            for _ in 0..(1 + company % 5) {
                metastate.increment("company_id", &company);
            }
        }
    }
    (db_pools, metastate, path)
}

/// Run the selection query and fetch just the first chunk (reserving one chunk streams
/// and stops after the first row).
async fn select_first_chunk(db_pools: &db::DBPools, strategy: &Strategy, metastate: &MetaState) {
    let mut conn = db_pools.reader_conn().await.unwrap();
    let mut query = sqlx::QueryBuilder::default();
    let chunk: Option<Chunk> = strategy
        .build_query(&mut query, metastate)
        .build_query_as()
        .fetch(conn.get_inner())
        .try_next()
        .await
        .unwrap();
    assert!(chunk.is_some(), "backlog should always have a pending chunk");
}

fn median_us(mut samples: Vec<f64>) -> f64 {
    samples.sort_by(|a, b| a.partial_cmp(b).unwrap());
    samples[samples.len() / 2]
}

fn main() {
    let runtime = tokio::runtime::Runtime::new().unwrap();

    let csv_path = concat!(env!("CARGO_MANIFEST_DIR"), "/../target/chunks_select_bench.csv");
    let mut csv = std::fs::File::create(csv_path).unwrap();
    writeln!(csv, "shape,strategy,backlog_size,median_us").unwrap();

    for shape in ["few_submissions_many_chunks", "many_submissions_few_chunks"] {
        for (strategy_label, strategy) in strategies() {
            for &size in BACKLOG_SIZES {
                let (db_pools, metastate, path) = runtime.block_on(seed(shape, size));
                let mut samples = Vec::new();
                for i in 0..WARMUP + SAMPLES {
                    let start = Instant::now();
                    runtime.block_on(select_first_chunk(&db_pools, &strategy, &metastate));
                    if i >= WARMUP {
                        samples.push(start.elapsed().as_secs_f64() * 1e6);
                    }
                }
                let median = median_us(samples);
                println!("{shape} {strategy_label} size={size}: {median:.1}us");
                writeln!(csv, "{shape},\"{strategy_label}\",{size},{median}").unwrap();

                drop(db_pools);
                let _ = std::fs::remove_file(&path);
                let _ = std::fs::remove_file(path.with_extension("sqlite-wal"));
                let _ = std::fs::remove_file(path.with_extension("sqlite-shm"));
            }
        }
    }
}
