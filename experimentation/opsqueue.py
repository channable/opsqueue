#!/usr/bin/env python
import sqlite3

from contextlib import contextmanager
from datetime import datetime
from fastapi import FastAPI
from pydantic import BaseModel
from fastapi.testclient import TestClient

app = FastAPI()
client = TestClient(app)
DB_NAME = "opsqueue.db"


@contextmanager
def db(db_name):
    """
    Use like this:

    with db(db_name) as cursor:
        cursor.execute('select * from bla')
    """
    conn = sqlite3.connect(db_name)
    try:
        cur = conn.cursor()
        yield cur
    except Exception as e:
        # do something with exception
        conn.rollback()
        raise e
    else:
        conn.commit()
    finally:
        conn.close()


class Chunk(BaseModel):
    # Note: (submission_directory, chunk_id) must be unique
    submission_directory: str
    chunk_id: int
    # created_at: datetime


class Submission(BaseModel):
    # The submission directory is a path on GCS like e.g. "gs://opsqueue_submissions/image_editing_chunks/<submission_id>/
    # Note: The "submission_directory" is also the unique id of a submission!
    submission_directory: str
    chunk_count: int
    # Metadata can be any JSON
    # metadata: dict

    # This field should be created by the database
    # created_at: datetime


@app.get("/")
async def root():
    return {"msg": "Hello World"}


def submission_factory(cursor, row):
    """
    Build a Pydantic Submission object from a raw sqlite3.Row object.
    """
    fields = [column[0] for column in cursor.description]
    return Submission(**{k: v for k, v in zip(fields, row)})


@app.get("/submissions")
async def get_submissions():
    """
    Return all submissions stored in the DB.
    """
    connection = sqlite3.connect(DB_NAME)

    # Returns every row as a dict (for nicer ergonomics)
    connection.row_factory = submission_factory
    cursor = connection.cursor()

    rows = cursor.execute("SELECT * FROM submissions")

    return {"submissions": list(rows)}


@app.post("/submissions")
async def post_submissions(submission: Submission) -> Submission:
    """
    Insert a new submission into the DB.
    """
    with db(DB_NAME) as cursor:
        try:
            cursor.execute(
                "INSERT INTO submissions VALUES (?, ?)",
                (submission.submission_directory, submission.chunk_count),
            )
        except sqlite3.IntegrityError as e:
            print(f"Invalid submission: {e}")

    return submission


def create_db(filename: str) -> None:
    """
    Create a SQLite database for Opsqueue.

    We create one table for submissions and one table for chunks.

    Does nothing if the file already exists.
    """
    con = sqlite3.connect(filename)
    cur = con.cursor()
    # TODO: Add metadata JSON field
    cur.execute(
        """CREATE TABLE submissions(submission_directory TEXT PRIMARY KEY, chunk_count INTEGER)"""
    )
    cur.execute(
        """CREATE TABLE chunks(
submission_directory TEXT,
chunk_id INTEGER,
FOREIGN KEY(submission_directory) REFERENCES submissions(submission_directory),
PRIMARY KEY (submission_directory, chunk_id)
)"""
    )


def main() -> None:
    print("Starting up Opsqueue...")
    create_db("opsqueue.db")


if __name__ == "__main__":
    main()


def test_read_main():
    response = client.get("/")
    assert response.status_code == 200
    assert response.json() == {"msg": "Hello World"}
