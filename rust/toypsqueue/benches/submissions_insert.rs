use criterion::{
    criterion_group, criterion_main, AxisScale, BenchmarkId, Criterion,
    PlotConfiguration, Throughput,
};
use sqlx::{Connection, SqliteConnection};
use toypsqueue::{
    chunk::{self, Chunk},
    submission::{self, Submission},
};

const DATABASE_URL: &str = "sqlite://opsqueue.db";
// const SIZES: [u64; 8] = [1, 10, 20, 50, 100, 200, 500, 1000];
const SIZES: [u64; 4] = [100, 1000, 10_000, 100_000];

pub async fn create_fake_submission_single_chunk(
    conn: &mut SqliteConnection,
    submission: Submission,
    chunks: Vec<Chunk>,
) {
    let _ = conn
        .transaction(|tx| {
            Box::pin(async move {
                for chunk in chunks {
                    chunk::insert_chunk(chunk, &mut **tx).await?;
                }
                submission::insert_submission(submission, &mut **tx).await
            })
        })
        .await;
}

pub async fn create_fake_submission_multi_chunk(
    conn: &mut SqliteConnection,
    submission: Submission,
    chunks: Vec<Chunk>,
) {
    let _ = conn
        .transaction(|tx| {
            Box::pin(async move {
                chunk::insert_many_chunks(&chunks, &mut **tx).await?;
                submission::insert_submission(submission, &mut **tx).await
            })
        })
        .await;
}

pub async fn create_fake_submission_multi_transaction(
    conn: &mut SqliteConnection,
    submission: Submission,
    chunks: Vec<Chunk>,
) {
    let chunks_per_transaction = 10_000;
    for transaction_chunks in chunks.chunks(chunks_per_transaction).map(|c| c.to_vec()) {
        let _ = conn
            .transaction(|tx| {
                Box::pin(
                    async move { chunk::insert_many_chunks(&transaction_chunks, &mut **tx).await },
                )
            })
            .await;
    }

    let _ = conn
        .transaction(|tx| {
            Box::pin(async move { submission::insert_submission(submission, &mut **tx).await })
        })
        .await;
}

pub fn build_fake_submission(submission_size: &u64) -> (Submission, Vec<Chunk>) {
    let vec = (0..*submission_size)
        .map(|num| format!("{}", num).into())
        .collect();
    Submission::from_vec(vec, None).unwrap()
}

pub fn bench_insert_submission(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let mut group = c.benchmark_group("insert_submission");
    for size in SIZES {
        group.throughput(Throughput::Elements(size));
        // group.bench_with_input(BenchmarkId::new("single_chunk", size), &size, |b, size| {
        //     b.to_async(&runtime).iter_custom(|iters| async move {
        //         let mut db = sqlx::SqliteConnection::connect(DATABASE_URL)
        //             .await
        //             .expect("Could not connect to sqlite DB");

        //         let start = std::time::Instant::now();
        //         for _i in 0..iters {
        //             black_box({
        //                 let (submission, chunks) = build_fake_submission(size);
        //                 let _ = create_fake_submission_single_chunk(&mut db, submission, chunks).await;
        //             })
        //         }
        //         start.elapsed()
        //     });
        // });
        group.bench_with_input(BenchmarkId::new("multi_chunk", size), &size, |b, size| {
            b.to_async(&runtime).iter_custom(|iters| async move {
                let mut db = sqlx::SqliteConnection::connect(DATABASE_URL)
                    .await
                    .expect("Could not connect to sqlite DB");

                let start = std::time::Instant::now();
                for _i in 0..iters {
                    {
                        let (submission, chunks) = build_fake_submission(size);
                        {
                            create_fake_submission_multi_chunk(&mut db, submission, chunks).await;
                        };
                    };
                }
                start.elapsed()
            });
        });
        group.bench_with_input(
            BenchmarkId::new("multi_transaction", size),
            &size,
            |b, size| {
                b.to_async(&runtime).iter_custom(|iters| async move {
                    let mut db = sqlx::SqliteConnection::connect(DATABASE_URL)
                        .await
                        .expect("Could not connect to sqlite DB");

                    let start = std::time::Instant::now();
                    for _i in 0..iters {
                        {
                            let (submission, chunks) = build_fake_submission(size);
                            {
                                create_fake_submission_multi_transaction(
                                    &mut db, submission, chunks,
                                )
                                .await;
                            };
                        };
                    }
                    start.elapsed()
                });
            },
        );
    }
    group.plot_config(PlotConfiguration::default().summary_scale(AxisScale::Logarithmic));
    group.finish();
}

criterion_group!(benches, bench_insert_submission);
criterion_main!(benches);
