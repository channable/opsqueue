// use toypsqueue::chunk::Chunk;
// use toypsqueue::persistence::Persistence;
// use toypsqueue::submission::Submission;

use sqlx::{migrate::MigrateDatabase, Sqlite, SqlitePool, Row, FromRow};
use toypsqueue::chunk::Chunk;

const DATABASE_URL: &str = "sqlite://opsqueue.db";


#[tokio::main]
async fn main () {
    ensure_db_exists().await;

    let db = SqlitePool::connect(DATABASE_URL).await.expect("Could not connect to sqlite DB");
    ensure_db_migrated(&db).await;


}

async fn ensure_db_exists() {
    if !Sqlite::database_exists(DATABASE_URL).await.unwrap_or(false) {
        println!("Creating backing sqlite DB {}", DATABASE_URL);
        Sqlite::create_database(DATABASE_URL).await.expect("Could not create backing sqlite DB");
        println!("Finished creating backing sqlite DB");
    } else {
        println!("Starting up using existing sqlite DB {}", DATABASE_URL);
    }
}

async fn ensure_db_migrated(db: &SqlitePool) {
    println!("Migrating backing DB");
    sqlx::migrate!("./migrations").run(db).await.expect("DB migrations failed");
    println!("Finished migrating backing DB");
}

// #[derive(Clone, FromRow, Debug)]
// struct User {
//   id: i64,
//   name: String,
// }




// #[tokio::main]
// async fn main_old() {
//     if !Sqlite::database_exists(DATABASE_URL).await.unwrap_or(false) {
//         println!("Creating database {}", DATABASE_URL);
//         match Sqlite::create_database(DATABASE_URL).await {
//             Ok(_) => println!("Create db success"),
//             Err(error) => panic!("error: {}", error),
//         }
//     } else {
//         println!("Database already exists");
//     }
//     let db = SqlitePool::connect(DATABASE_URL).await.unwrap();
//     // let result = sqlx::query("CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY NOT NULL, name VARCHAR(250) NOT NULL);").execute(&db).await.unwrap();
//     // println!("Create user table result: {:?}", result);

//     let result = 
//       sqlx::query!("SELECT rowid, name FROM sqlite_schema WHERE type='table' AND name NOT LIKE 'sqlite_%';",)
//       .fetch_all(&db)
//       .await
//       .unwrap();

//     for row in result.iter() {
//         println!("[{}]: {:?}", row.rowid, row.name.as_ref().unwrap());
//     }

//     println!("Query result: {:?}", result);

//     // let users = 
//     //     sqlx::query_as!(User, "SELECT id, name FROM users")
//     //     .fetch_all(&db)
//     //     .await
//     //     .unwrap();

//     // for user in users {
//     //     println!( "[{}] name: {}", user.id, &user.name);
//     // }

//     for _ in 1..10 {
//         let now = std::time::Instant::now();
//         let users = select_random_chunks(&db, 10).await;
//         let elapsed = now.elapsed();
//         println!("Random chunks: {users:?}, fetching took {elapsed:?}");
//     }

//     for _ in 1..10 {
//         let now = std::time::Instant::now();
//         let users = select_oldest_chunks(&db, 10).await;
//         let elapsed = now.elapsed();
//         println!("Oldest chunks: {users:?}, fetching took {elapsed:?}");
//     }

//     // let delete_result = 
//     //     sqlx::query!("DELETE FROM users WHERE name = $1", "bobby")
//     //     .execute(&db)
//     //     .await
//     //     .unwrap();

//     // println!("Delete result: {:?}", delete_result);
// }

// async fn select_oldest_chunks(db: &sqlx::Pool<Sqlite>, count: i32) -> Vec<(Option<i64>, Option<i64>)>{
//     sqlx::query!("SELECT * FROM chunks ORDER BY submission_id ASC, id ASC LIMIT $1", count)
//     .fetch_all(db)
//     .await
//     .unwrap()
//     .iter()
//     .map(|row| (row.submission_id, row.id))
//     .collect()
// }

// async fn select_random_chunks(db: &sqlx::Pool<Sqlite>, count: i32) -> Vec<(Option<i64>, Option<i64>)> {
//     let count_div10 = count / 2;
//     sqlx::query!("SELECT submission_id, id, uri FROM chunks JOIN
//     (SELECT rowid as rid FROM chunks
//         WHERE random() % $1 = 0  -- Reduce rowids by Nx
//         LIMIT $2) AS srid
//     ON chunks.rowid = srid.rid;", count_div10, count)
//     .fetch_all(db)
//     .await
//     .unwrap()
//     .iter()
//     .map(|row| (row.submission_id, row.id))
//     .collect()
//     // .fetch_all(db)
//     // .await
// }

// // fn main() -> rusqlite::Result<()> {
// //     // let mut conn = rusqlite::Connection::open_in_memory()?;
// //     let conn = rusqlite::Connection::open("./example_db.db3")?;
// //     conn.migrate()?;

// //     std::thread::scope(|s| {
// //         for _ in 0..10 {
// //             s.spawn(move || {
// //                 for sid in 0..100 {
// //                     let mut conn = rusqlite::Connection::open("./example_db.db3").unwrap();

// //                     conn.pragma_update(None, "busy_timeout", "50000").unwrap();
// //                     let _ = write_fake_submission(&mut conn, sid, 10000).unwrap();

// //                     println!("tid {:?}, inserted submission {} ", std::thread::current().id(), sid);
// //                 }
// //             });
// //         }

// //     });

// //     // println!("Hello, world!");

// //     // let conn = rusqlite::Connection::open_in_memory()?;
// //     // conn.execute("CREATE TABLE IF NOT EXISTS submissions (id INTEGER PRIMARY KEY AUTOINCREMENT, chunks_total INTEGER, chunks_done INTEGER, metadata BLOB);", ())?;

// //     // let example = Submission {id: 0, chunks_total: 100, chunks_done: 0, metadata: None};
// //     // conn.execute("INSERT INTO submissions (chunks_total, chunks_done, metadata) VALUES (?1, ?2, ?3)", (&example.chunks_total, &example.chunks_done, &example.metadata))?;

// //     let mut iter_query = conn.prepare("SELECT submission_id, id, uri FROM chunks;")?;
// //     let iter = iter_query.query_map([], |row| {
// //         Ok(Chunk {
// //             submission_id: row.get(0)?,
// //             id: row.get(1)?,
// //             uri: row.get(2)?,
// //         })
// //     })?;

// //     for (index, sub) in iter.enumerate() {
// //         if index % 100 == 99 {
// //             println!("Found 100 chunks, 100th chunk: {:?}", sub?);
// //         }
// //     }

// //     Ok(())
// // }

// // fn write_fake_submission(conn: &mut rusqlite::Connection, submission_index: usize, size: usize) -> rusqlite::Result<()> {
// //     let vec = (1..size).map(|num| num.to_string().into()).collect();
// //     let (submission, chunks) = Submission::from_vec(vec, None);
// //     for block in chunks.chunks(1000) {
// //         loop {
// //             let res = conn.atomically(|tx| {
// //                 for chunk in block {
// //                     // println!("{:?}, Inserting a chunk of chunks for submission with index {}", std::thread::current().id(), submission_index);
// //                     tx.insert_chunk(&chunk)?;
// //                 }
// //                 Ok(())
// //             });
// //             std::thread::sleep_ms(1);
// //             match res {
// //                 Ok(_) => break,
// //                 Err(_) => continue,
// //             }
// //         }
// //     }
// //     let res = conn.atomically(|tx| {
// //         tx.insert_submission(&submission)?;
// //         Ok(())
// //     })?;
// //     // })?;
// //     Ok(())
// // }
