use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use toypsqueue::chunk;

const DATABASE_URL: &str = "sqlite://opsqueue.db";


pub fn select_newest(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    const DATABASE_URL: &str = "sqlite://opsqueue.db";
    let db = runtime.block_on(async {
        sqlx::SqlitePool::connect(DATABASE_URL).await.expect("Could not connect to sqlite DB")
    });


    let mut group = c.benchmark_group("select_newest");
    for size in [1, 10, 100, 1000] {
        group.throughput(Throughput::Elements(size));
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
            b.to_async(&runtime).iter(|| async {
                chunk::select_newest_chunks(&db, size as u32).await
            });
        });
    }
    group.finish();
}


criterion_group!(benches, select_newest);
criterion_main!(benches);
