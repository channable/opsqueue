use criterion::{
    black_box, criterion_group, criterion_main, AxisScale, BenchmarkId, Criterion,
    PlotConfiguration, Throughput,
};
use sqlx::Connection;
use toypsqueue::chunk;

const DATABASE_URL: &str = "sqlite://opsqueue.db";
const SIZES: [u64; 8] = [1, 10, 20, 50, 100, 200, 500, 1000];

pub fn select_newest(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let mut group = c.benchmark_group("select_newest");
    for size in SIZES {
        group.throughput(Throughput::Elements(size));
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
            b.to_async(&runtime).iter_custom(|iters| async move {
                let mut db = sqlx::SqliteConnection::connect(DATABASE_URL)
                    .await
                    .expect("Could not connect to sqlite DB");

                let start = std::time::Instant::now();
                for _i in 0..iters {
                    {
                        let _ = black_box(chunk::select_newest_chunks(&mut db, size as u32).await);
                    };
                }
                start.elapsed()
            });
        });
    }
    group.plot_config(PlotConfiguration::default().summary_scale(AxisScale::Logarithmic));
    group.finish();
}
pub fn select_oldest(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let mut group = c.benchmark_group("select_oldest");
    for size in SIZES {
        group.throughput(Throughput::Elements(size));
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
            b.to_async(&runtime).iter_custom(|iters| async move {
                let mut db = sqlx::SqliteConnection::connect(DATABASE_URL)
                    .await
                    .expect("Could not connect to sqlite DB");

                let start = std::time::Instant::now();
                for _i in 0..iters {
                    {
                        let _ = black_box(chunk::select_oldest_chunks(&mut db, size as u32).await);
                    };
                }
                start.elapsed()
            });
        });
    }
    group.plot_config(PlotConfiguration::default().summary_scale(AxisScale::Logarithmic));
    group.finish();
}

pub fn select_random(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let mut group = c.benchmark_group("select_random");
    for size in SIZES {
        group.sample_size(10);
        group.throughput(Throughput::Elements(size));
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
            b.to_async(&runtime).iter_custom(|iters| async move {
                let mut db = sqlx::SqliteConnection::connect(DATABASE_URL)
                    .await
                    .expect("Could not connect to sqlite DB");

                let start = std::time::Instant::now();
                for _i in 0..iters {
                    {
                        let _ = black_box(chunk::select_random_chunks(&mut db, size as u32).await);
                    };
                }
                start.elapsed()
            });
        });
    }
    group.plot_config(PlotConfiguration::default().summary_scale(AxisScale::Logarithmic));
    group.finish();
}

criterion_group!(benches, select_newest, select_oldest, select_random);
criterion_main!(benches);
