/// FOR EACH shape:
///   FOR EACH strategy:
///     FOR EACH amount of chunks:
///
///         // Setup
///         Create fresh temporary SQLite database
///         Insert chunks, assigning them to companies/submissions based on `shape`
///
///         // MEASURE
///         FOR _ in (SAMPLES + WARMUP):
///             Start Timer
///             Execute SQL Query -> Fetch EXACTLY ONE chunk
///             Stop Timer
///             IF (not warmup): save duration
///
///         // REPORT & CLEANUP
///         Calculate stats
///         Write result to terminal and CSV
///         Delete temporary database files
use opsqueue::common::StrategicMetadataMap;
use opsqueue::common::chunk::{ChunkId, ChunkSize};
use opsqueue::common::submission::db::insert_submission_from_chunks;
use opsqueue::consumer::dispatcher::Dispatcher;
use opsqueue::consumer::strategy::Strategy;
use opsqueue::db::{self};
use std::io::Write;
use std::num::NonZero;
use std::path::PathBuf;
use std::time::{Duration, Instant};

// Human readable name for an OpsQueue strategy.
type StrategyName = &'static str;

// Shape of the data we are inserting.
#[derive(Debug, Eq, Ord, PartialEq, PartialOrd)]
enum Shape {
    FewSubmissionsManyChunks,
    ManySubmissionsFewChunks,
    Realistic,
}

const CHUNKS_PER_COMPANY: u64 = CHUNKS_PER_SUBMISSION * SUBMISSIONS_PER_COMPANY;
const CHUNKS_PER_SUBMISSION: u64 = 1024;
// Increase in the total amount of chunks for the `Shape::Realistic` strategy.
const CHUNKS_STEP: usize = 500_000;
// Amount of companies in Channable 23 July 2026.
const COMPANIES: u64 = 93221;
// We use this as an approximation of the amount of submissions.
const EXPORTS: u64 = 303_262;
const MAX_CHUNKS: u64 = 10_000_001;
// Unused: amount of projects in Channable 23 July 2026
// const PROJECTS: u64 = 166774;
// The amount of samples to collect per iteration of the
// shape/strategy/amount_of_chunks main loop. Note that each "sample" requires a
// number of reservations to be performed. See `bench_strategy` for more info.
const SAMPLES_PER_DATUM: usize = 10;
const SUBMISSIONS_PER_COMPANY: u64 = EXPORTS / COMPANIES;
// The amount of reservations to collect a single sample.
// Note that the time between these is NOT INDEPENDENT:
// the earlier reservations affect the later ones.
const RESERVATIONS_IN_SAMPLE: usize = 50;
// The amount of reservations to perform before we begin collecting a sample.
const RESERVATIONS_IN_WARMUP: usize = 5;

#[derive(Debug)]
pub struct BenchStats {
    pub median: f64, // Median of medians
    pub p10: f64,    // Lower bound of medians
    pub p90: f64,    // Upper bound of medians
}

#[allow(clippy::missing_panics_doc)]
impl BenchStats {
    #[must_use]
    pub fn new(runs: Vec<Vec<f64>>) -> Self {
        // Calculate the median for each individual run
        let mut run_medians: Vec<f64> = runs
            .into_iter()
            .filter_map(|mut run_samples| {
                if run_samples.is_empty() {
                    None
                } else {
                    run_samples.sort_by(|a, b| a.partial_cmp(b).unwrap());
                    Some(run_samples[run_samples.len() / 2])
                }
            })
            .collect();
        if run_medians.is_empty() {
            return BenchStats {
                p10: 0.,
                median: 0.,
                p90: 0.,
            };
        }
        // Sort the N medians to find our distribution.
        run_medians.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let len = run_medians.len();
        BenchStats {
            p10: run_medians[len * 10 / 100],
            median: run_medians[len / 2],
            p90: run_medians[len * 90 / 100],
        }
    }
}

/// Maximum amount of chunks for the bench run.
///
/// This is based on the shape of the data, as that can dramatically affect
/// run-time, and for some shapes we can push the total chunks higher without
/// waiting too long.
fn max_chunks_by_shape(shape: &Shape) -> Vec<u64> {
    let mut vector: Vec<u64> = vec![
        100, 500, 1_000, 2_000, 4_000, 6_000, 8_000, 10_000, 15_000, 20_000, 30_000,
    ];
    if shape == &Shape::Realistic {
        let max = MAX_CHUNKS.min(COMPANIES * CHUNKS_PER_COMPANY);
        vector.extend((50_000..=max).step_by(CHUNKS_STEP));
    }
    vector
}

/// All the strategies we are benchmarking.
fn strategies() -> [(StrategyName, Strategy); 2] {
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

/// (total companies, `submissions_per_company`, chunks per submission) for a shape and total chunks.
fn layout(shape: &Shape, total_chunks: u64) -> (u64, u64, u64) {
    match shape {
        Shape::ManySubmissionsFewChunks => (total_chunks, 1, 1),
        Shape::FewSubmissionsManyChunks => {
            let submissions = 10.min(total_chunks.max(1));
            (submissions, 1, total_chunks.div_ceil(submissions))
        }
        Shape::Realistic => {
            let companies = total_chunks.div_ceil(CHUNKS_PER_COMPANY);
            (companies, SUBMISSIONS_PER_COMPANY, CHUNKS_PER_SUBMISSION)
        }
    }
}

/// Seeds a fresh DB with chunks according to a given `Shape`.
async fn seed(shape: &Shape, total_chunks: u64) -> (db::DBPools, Dispatcher, PathBuf) {
    let (total_companies, submissions_per_company, chunks_per_submission) =
        layout(shape, total_chunks);
    println!(
        "    Companies: {total_companies:<30}\n    Submissions per company: {submissions_per_company}\n    Chunks per submission: {chunks_per_submission}"
    );
    let db_path = std::env::temp_dir().join("opsqueue_bench.sqlite");
    // Remove the DB before the run.
    let _ = std::fs::remove_file(&db_path);
    let db_pools = db::open_and_setup(db_path.to_str().unwrap(), NonZero::new(16).unwrap()).await;
    let mut conn = db_pools.writer_conn().await.unwrap();
    let dispatcher = Dispatcher::new(Duration::from_mins(1_000_000));
    for company_id in 0..total_companies {
        for _ in 0..submissions_per_company {
            let mut metadata = StrategicMetadataMap::default();
            metadata.insert("company_id".to_string(), i64::try_from(company_id).unwrap());
            let chunks = vec![Some(b"x".to_vec()); usize::try_from(chunks_per_submission).unwrap()];
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
        }
    }
    (db_pools, dispatcher, db_path)
}

/// Runs the selection query and fetches just the first chunk.
async fn fetch_and_reserve_chunk(
    db_pools: &db::DBPools,
    strategy: Strategy,
    dispatcher: &Dispatcher,
) -> ChunkId {
    use tokio::sync::mpsc::unbounded_channel;
    let (notifier, _) = unbounded_channel();
    let reserved = dispatcher
        .fetch_and_reserve_chunks(db_pools.reader_pool(), strategy, 1, &notifier)
        .await
        .unwrap();
    if let [(chunk, _)] = reserved.as_slice() {
        ChunkId::from((chunk.submission_id, chunk.chunk_index))
    } else {
        panic!(
            "Yowza! Expected exactly 1 chunk, but got {}",
            reserved.len()
        );
    }
}

/// Executes reservations for a given DB state and returns `BenchStats`.
async fn bench_strategy(
    db_pools: &db::DBPools,
    strategy: &Strategy,
    dispatcher: &Dispatcher,
) -> BenchStats {
    let mut all_reservation_durations: Vec<Vec<f64>> = Vec::with_capacity(SAMPLES_PER_DATUM);
    // For each of the "samples" we want to collect.
    for _ in 0..SAMPLES_PER_DATUM {
        let mut sample_reservation_durations = Vec::with_capacity(RESERVATIONS_IN_SAMPLE);
        let mut chunks_reserved = Vec::new();
        // For each sample, reserve N chunks (some are warmups).
        for i in 0..(RESERVATIONS_IN_WARMUP + RESERVATIONS_IN_SAMPLE) {
            let start = Instant::now();
            let chunk_id = fetch_and_reserve_chunk(db_pools, strategy.clone(), dispatcher).await;
            if i >= RESERVATIONS_IN_WARMUP {
                sample_reservation_durations.push(start.elapsed().as_secs_f64() * 1e6);
            }
            chunks_reserved.push(chunk_id);
        }
        all_reservation_durations.push(sample_reservation_durations);
        // Reset the queue state for the next run
        let mut conn = db_pools.writer_conn().await.unwrap();
        for chunk_id in chunks_reserved {
            dispatcher
                .finish_reservation(&mut conn, chunk_id, false)
                .await;
        }
    }
    BenchStats::new(all_reservation_durations)
}

fn main() {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let csv_path = PathBuf::from("chunks_select_bench.csv");
    if let Some(parent) = csv_path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    let mut csv = std::fs::File::create(&csv_path)
        .unwrap_or_else(|e| panic!("Failed to create CSV at {}: {}", csv_path.display(), e));
    writeln!(csv, "shape,strategy,backlog_size,p10_us,median_us,p90_us").unwrap();
    println!(
        "{:<30} {:<35} {:<12} {:<10} {:<15}",
        "SHAPE", "STRATEGY", "SIZE", "MEDIAN", "P10 / P90"
    );
    println!("{}", "-".repeat(105));
    for shape in &[
        Shape::FewSubmissionsManyChunks,
        Shape::ManySubmissionsFewChunks,
        Shape::Realistic,
    ] {
        for (strategy_label, strategy) in strategies() {
            for size in max_chunks_by_shape(shape) {
                let (db_pools, dispatcher, path) = runtime.block_on(seed(shape, size));
                let stats = runtime.block_on(bench_strategy(&db_pools, &strategy, &dispatcher));
                let bounds = format!("{:.1} / {:.1}", stats.p10, stats.p90);
                println!(
                    "{:<30} {:<35} {:<12} {:<10.1} {:<15}",
                    format!("{shape:?}"),
                    strategy_label,
                    size,
                    stats.median,
                    bounds
                );
                writeln!(
                    csv,
                    "{shape:?},\"{strategy_label}\",{size},{:.1},{:.1},{:.1}",
                    stats.p10, stats.median, stats.p90
                )
                .unwrap();
                drop(db_pools);
                let _ = std::fs::remove_file(&path);
                let _ = std::fs::remove_file(path.with_extension("sqlite-wal"));
                let _ = std::fs::remove_file(path.with_extension("sqlite-shm"));
            }
        }
    }
}
