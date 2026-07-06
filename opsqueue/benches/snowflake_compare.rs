use std::hint::black_box;
use std::time::Instant;

use criterion::{criterion_group, criterion_main, Criterion};
use opsqueue::common::submission::SubmissionId;
use snowflaked::sync::Generator;

static ORACLE_GENERATOR: Generator = Generator::new(0);

fn bench_single_thread(c: &mut Criterion) {
    let mut group = c.benchmark_group("snowflake_single_thread");

    group.bench_function("custom_submission_id_new", |b| {
        b.iter(|| {
            black_box(SubmissionId::new());
        })
    });

    group.bench_function("snowflaked_generate_u64", |b| {
        b.iter(|| {
            let id: u64 = ORACLE_GENERATOR.generate();
            black_box(id);
        })
    });

    group.finish();
}

fn generate_custom_parallel(threads: usize, ids_per_thread: usize) {
    let workers: Vec<_> = (0..threads)
        .map(|_| {
            std::thread::spawn(move || {
                for _ in 0..ids_per_thread {
                    black_box(SubmissionId::new());
                }
            })
        })
        .collect();

    for worker in workers {
        worker
            .join()
            .expect("parallel custom benchmark worker panicked");
    }
}

fn generate_oracle_parallel(threads: usize, ids_per_thread: usize) {
    let workers: Vec<_> = (0..threads)
        .map(|_| {
            std::thread::spawn(move || {
                for _ in 0..ids_per_thread {
                    let id: u64 = ORACLE_GENERATOR.generate();
                    black_box(id);
                }
            })
        })
        .collect();

    for worker in workers {
        worker
            .join()
            .expect("parallel oracle benchmark worker panicked");
    }
}

fn bench_parallel(c: &mut Criterion) {
    let mut group = c.benchmark_group("snowflake_parallel");
    let threads = 4;
    let ids_per_thread = 10_000;

    group.bench_function("custom_submission_id_new_threads_4", |b| {
        b.iter_custom(|iters| {
            let start = Instant::now();
            for _ in 0..iters {
                generate_custom_parallel(threads, ids_per_thread);
            }
            start.elapsed()
        });
    });

    group.bench_function("snowflaked_generate_u64_threads_4", |b| {
        b.iter_custom(|iters| {
            let start = Instant::now();
            for _ in 0..iters {
                generate_oracle_parallel(threads, ids_per_thread);
            }
            start.elapsed()
        });
    });

    group.finish();
}

criterion_group!(benches, bench_single_thread, bench_parallel);
criterion_main!(benches);
