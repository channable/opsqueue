// use toypsqueue::chunk::Chunk;
// use toypsqueue::persistence::Persistence;
// use toypsqueue::submission::Submission;

use sqlx::{Acquire, SqlitePool};
use tokio::task::JoinSet;
use toypsqueue::{
    chunk, db_connect_pool, ensure_db_exists, ensure_db_migrated,
    submission::{self, Submission},
};

#[tokio::main]
async fn main() {
    ensure_db_exists().await;

    let db = db_connect_pool().await;
    ensure_db_migrated(&db).await;

    // Play with the numbers here to see how this affects Sqlite write performance
    // or the characteristics of Litestream:
    create_fake_submissions(&db, 100_000, 1000, 1).await;
}

async fn create_fake_submissions(
    db: &SqlitePool,
    submission_size: u32,
    n_submissions_per_writer: u32,
    n_writers: u32,
) {
    let mut set: JoinSet<()> = JoinSet::new();
    for i in 0..n_writers {
        let mut conn = db.acquire().await.unwrap();
        set.spawn(async move {
            for n in 0..n_submissions_per_writer {
                println!(
                    "{i}: Starting on submission {} ({} chunks)",
                    n, submission_size
                );
                let mut tx = conn.begin().await.unwrap();
                let vec = (0..submission_size)
                    .map(|num| format!("{}", num).into())
                    .collect();
                let (submission, chunks) = Submission::from_vec(vec, None).unwrap();

                let _ = chunk::insert_many_chunks(&chunks, &mut *tx).await;
                let _ = submission::insert_submission(submission, &mut *tx).await;

                tx.commit().await.unwrap();
                println!("{i}: Submitted submission {n}");
                tokio::task::yield_now().await;
            }
        });
    }

    while let Some(res) = set.join_next().await {
        println!("Thread result: {:?}", res);
    }
}
