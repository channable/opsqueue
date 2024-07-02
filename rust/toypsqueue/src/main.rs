#[derive(Debug)]
struct Submission {
    id: i64,
    chunks_total: i64,
    chunks_done: i64,
    metadata: Option<Vec<u8>>,
}

fn main() -> rusqlite::Result<()> {
    println!("Hello, world!");

    let conn = rusqlite::Connection::open_in_memory()?;
    conn.execute("CREATE TABLE IF NOT EXISTS submissions (id INTEGER PRIMARY KEY AUTOINCREMENT, chunks_total INTEGER, chunks_done INTEGER, metadata BLOB);", ())?;

    let example = Submission {id: 0, chunks_total: 100, chunks_done: 0, metadata: None};
    conn.execute("INSERT INTO submissions (chunks_total, chunks_done, metadata) VALUES (?1, ?2, ?3)", (&example.chunks_total, &example.chunks_done, &example.metadata))?;

    let mut iter_query = conn.prepare("SELECT id, chunks_total, chunks_done, metadata FROM submissions;")?;
    let iter = iter_query.query_map([], |row| Ok(Submission {
        id: row.get(0)?,
        chunks_total: row.get(1)?,
        chunks_done: row.get(2)?,
        metadata: row.get(3)?,
    }))?;

    for sub in iter {
        println!("Found submission: {:?}", sub?);
    }

    Ok(())
}
