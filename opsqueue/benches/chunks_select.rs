/// FOR EACH shape IN [`few_submissions`, `many_submissions`]:
///   FOR EACH strategy IN [Random, `PreferDistinct`]:
///     FOR EACH `backlog_size` IN [100, 500, 1000, ..., 30000]:
///
///         // 1. SETUP
///         Create fresh temporary SQLite database
///         Insert `backlog_size` chunks, assigning them to companies based on `shape`
///
///         // 2. SIMULATE LOAD (The "Traffic Jam")
///         FOR EACH company UP TO 256:
///             Add fake "in-flight" counts to MetaState for this company
///
///         // 3. MEASURE
///         Run 5 warmup iterations (fetch 1 chunk, discard time)
///
///         FOR iteration = 1 TO 30:
///             Start Timer
///             Execute SQL Query -> Fetch EXACTLY ONE chunk
///             Stop Timer
///             Save duration
///
///         // 4. REPORT & CLEANUP
///         Calculate median duration
///         Write result to terminal and CSV
///         Delete temporary database files
use futures::stream::TryStreamExt as _;
use opsqueue::common::StrategicMetadataMap;
use opsqueue::common::chunk::{Chunk, ChunkSize};
use opsqueue::common::submission::db::insert_submission_from_chunks;
use opsqueue::consumer::dispatcher::Dispatcher;
use opsqueue::consumer::strategy::Strategy;
use opsqueue::db::{self, Connection as _};
use std::io::Write;
use std::num::NonZero;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

const BACKLOG_SIZES: &[usize] = &[
    100, 500, 1_000, 2_000, 4_000, 6_000, 8_000, 10_000, 15_000, 20_000, 30_000,
];
const SAMPLES: usize = 30;
const WARMUP: usize = 5;
const MAX_IN_FLIGHT: usize = 256;

static DB_COUNTER: AtomicU64 = AtomicU64::new(0);

fn strategies() -> [(&'static str, Strategy); 2] {
    [
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

/// Seeds a fresh DB with a backlog shape and populates realistic in-flight `MetaState`.
async fn seed(shape: &str, total_chunks: usize) -> (db::DBPools, Dispatcher, PathBuf) {
    let (submissions, chunks_per_submission) = layout(shape, total_chunks);
    let unique = DB_COUNTER.fetch_add(1, Ordering::Relaxed);
    let path = std::env::temp_dir().join(format!("opsqueue_bench_{unique}.sqlite"));

    // Ensure clean state
    let _ = std::fs::remove_file(&path);

    let db_pools = db::open_and_setup(path.to_str().unwrap(), NonZero::new(16).unwrap()).await;
    let mut conn = db_pools.writer_conn().await.unwrap();
    let dispatcher = Dispatcher::new(Duration::from_mins(1));

    for company in 0..i64::try_from(submissions).unwrap() {
        let mut metadata = StrategicMetadataMap::default();
        metadata.insert("company_id".to_string(), company);
        let chunks = vec![Some(b"x".to_vec()); chunks_per_submission];

        insert_submission_from_chunks(
            None,
            chunks,
            None,
            metadata,
            ChunkSize::default(),
            &mut conn,
        )
        .await
        .unwrap();

        // THIS is what simulates a busy queue and causes the degradation curve!
        if usize::try_from(company).unwrap() < MAX_IN_FLIGHT {
            for _ in 0..=(company % 5) {
                dispatcher.metastate().increment("company_id", company);
            }
        }
    }

    (db_pools, dispatcher, path)
}

/// Runs the selection query and fetches just the first chunk.
async fn select_first_chunk(db_pools: &db::DBPools, strategy: &Strategy, dispatcher: &Dispatcher) {
    let mut conn = db_pools.reader_conn().await.unwrap();
    dispatcher.register_lookups(conn.get_inner()).await.unwrap();
    let mut query = sqlx::QueryBuilder::default();

    let chunk: Option<Chunk> = strategy
        .build_query(&mut query)
        .build_query_as()
        .fetch(conn.get_inner())
        .try_next()
        .await
        .unwrap();

    assert!(
        chunk.is_some(),
        "backlog should always have a pending chunk"
    );
}

fn median_us(mut samples: Vec<f64>) -> f64 {
    samples.sort_by(|a, b| a.partial_cmp(b).unwrap());
    samples[samples.len() / 2]
}

/// Executes warmup + sample runs for a given DB state and returns median latency in µs.
async fn bench_strategy(
    db_pools: &db::DBPools,
    strategy: &Strategy,
    dispatcher: &Dispatcher,
) -> f64 {
    let mut samples = Vec::with_capacity(SAMPLES);

    for i in 0..(WARMUP + SAMPLES) {
        let start = Instant::now();
        select_first_chunk(db_pools, strategy, dispatcher).await;

        if i >= WARMUP {
            samples.push(start.elapsed().as_secs_f64() * 1e6);
        }
    }

    median_us(samples)
}

fn get_csv_path() -> PathBuf {
    if let Ok(target_dir) = std::env::var("CARGO_TARGET_DIR") {
        PathBuf::from(target_dir).join("chunks_select_bench.csv")
    } else {
        // Fall back to target/ in the current working directory or manifest dir
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../target/chunks_select_bench.csv")
    }
}

fn main() {
    let runtime = tokio::runtime::Runtime::new().unwrap();

    // Setup CSV output
    let csv_path = get_csv_path();
    if let Some(parent) = csv_path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    let mut csv = std::fs::File::create(&csv_path)
        .unwrap_or_else(|e| panic!("failed to create CSV at {}: {}", csv_path.display(), e));

    writeln!(csv, "shape,strategy,backlog_size,median_us").unwrap();

    println!(
        "{:<30} {:<35} {:<12} {:<10}",
        "SHAPE", "STRATEGY", "SIZE", "MEDIAN (µs)"
    );
    println!("{}", "-".repeat(90));

    for shape in ["few_submissions_many_chunks", "many_submissions_few_chunks"] {
        for (strategy_label, strategy) in strategies() {
            for &size in BACKLOG_SIZES {
                let (db_pools, dispatcher, path) = runtime.block_on(seed(shape, size));

                let median = runtime.block_on(bench_strategy(&db_pools, &strategy, &dispatcher));

                println!("{shape:<30} {strategy_label:<35} {size:<12} {median:<10.1}");
                writeln!(csv, "{shape},\"{strategy_label}\",{size},{median:.1}").unwrap();

                // Cleanup DB files explicitly
                drop(db_pools);
                let _ = std::fs::remove_file(&path);
                let _ = std::fs::remove_file(path.with_extension("sqlite-wal"));
                let _ = std::fs::remove_file(path.with_extension("sqlite-shm"));
            }
        }
    }
}
