use toypsqueue::chunk::Chunk;
use toypsqueue::persistence::Persistence;
use toypsqueue::submission::Submission;

fn main() -> rusqlite::Result<()> {
    // let mut conn = rusqlite::Connection::open_in_memory()?;
    let conn = rusqlite::Connection::open("./example_db.db3")?;
    conn.migrate()?;

    std::thread::scope(|s| {
        for _ in 0..10 {
            s.spawn(move || {
                for sid in 0..100 {
                    let mut conn = rusqlite::Connection::open("./example_db.db3").unwrap();

                    conn.pragma_update(None, "busy_timeout", "50000").unwrap();
                    let _ = write_fake_submission(&mut conn, sid, 10000).unwrap();

                    println!("tid {:?}, inserted submission {} ", std::thread::current().id(), sid);
                }
            });
        }

    });

    // println!("Hello, world!");

    // let conn = rusqlite::Connection::open_in_memory()?;
    // conn.execute("CREATE TABLE IF NOT EXISTS submissions (id INTEGER PRIMARY KEY AUTOINCREMENT, chunks_total INTEGER, chunks_done INTEGER, metadata BLOB);", ())?;

    // let example = Submission {id: 0, chunks_total: 100, chunks_done: 0, metadata: None};
    // conn.execute("INSERT INTO submissions (chunks_total, chunks_done, metadata) VALUES (?1, ?2, ?3)", (&example.chunks_total, &example.chunks_done, &example.metadata))?;

    let mut iter_query = conn.prepare("SELECT submission_id, id, uri FROM chunks;")?;
    let iter = iter_query.query_map([], |row| {
        Ok(Chunk {
            submission_id: row.get(0)?,
            id: row.get(1)?,
            uri: row.get(2)?,
        })
    })?;

    for (index, sub) in iter.enumerate() {
        if index % 100 == 99 {
            println!("Found 100 chunks, 100th chunk: {:?}", sub?);
        }
    }

    Ok(())
}

fn write_fake_submission(conn: &mut rusqlite::Connection, submission_index: usize, size: usize) -> rusqlite::Result<()> {
    let vec = (1..size).map(|num| num.to_string().into()).collect();
    let (submission, chunks) = Submission::from_vec(vec, None);
    for block in chunks.chunks(1000) {
        loop {
            let res = conn.atomically(|tx| {
                for chunk in block {
                    // println!("{:?}, Inserting a chunk of chunks for submission with index {}", std::thread::current().id(), submission_index);
                    tx.insert_chunk(&chunk)?;
                }
                Ok(())
            });
            std::thread::sleep_ms(1);
            match res {
                Ok(_) => break,
                Err(_) => continue,
            }
        }
    }
    let res = conn.atomically(|tx| {
        tx.insert_submission(&submission)?;
        Ok(())
    })?;
    // })?;
    Ok(())
}
