// use toypsqueue::chunk::Chunk;
// use toypsqueue::persistence::Persistence;
// use toypsqueue::submission::Submission;

use sqlx::{migrate::MigrateDatabase, Acquire, FromRow, Row, Sqlite, SqlitePool};
use tokio::task::JoinSet;
use toypsqueue::{
    chunk::{self},
    submission::{self, Submission},
};

const DATABASE_URL: &str = "sqlite://opsqueue.db";

#[tokio::main]
async fn main() {
    ensure_db_exists().await;

    let db = SqlitePool::connect(DATABASE_URL)
        .await
        .expect("Could not connect to sqlite DB");
    ensure_db_migrated(&db).await;

    // Play with the numbers here to see how this affects Sqlite write performance
    // or the characteristics of Litestream:
    create_fake_submissions(&db, 1_000_000, 1000, 1).await;
}

async fn ensure_db_exists() {
    if !Sqlite::database_exists(DATABASE_URL).await.unwrap_or(false) {
        println!("Creating backing sqlite DB {}", DATABASE_URL);
        Sqlite::create_database(DATABASE_URL)
            .await
            .expect("Could not create backing sqlite DB");
        println!("Finished creating backing sqlite DB");
    } else {
        println!("Starting up using existing sqlite DB {}", DATABASE_URL);
    }
}

async fn ensure_db_migrated(db: &SqlitePool) {
    println!("Migrating backing DB");
    sqlx::migrate!("./migrations")
        .run(db)
        .await
        .expect("DB migrations failed");
    println!("Finished migrating backing DB");
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
                println!("{i}: Starting on submission {}", n);
                let mut tx = conn.begin().await.unwrap();
                let vec = (0..submission_size)
                    .map(|num| format!("{}", num).into())
                    .collect();
                let (submission, chunks) = Submission::from_vec(vec, None).unwrap();

                let _ = chunk::insert_many_chunks(&chunks, &mut *tx).await;
                // for chunk in chunks {
                //     let _ = chunk::insert_chunk(chunk, &mut *tx).await;
                // }
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
