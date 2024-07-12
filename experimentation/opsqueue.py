#!/usr/bin/env python
import sqlite3

from datetime import datetime
from fastapi import FastAPI
from pydantic import BaseModel
from fastapi.testclient import TestClient

app = FastAPI()
client = TestClient(app)


class Chunk(BaseModel):
    # Note: (submission_id, chunk_id) must be unique
    id: int
    submission_id: str
    gcs_path: str
    created_at: datetime


class Submission(BaseModel):
    # The submission directory is a path on GCS like e.g. "gs://opsqueue_submissions/image_editing_chunks/<submission_id>/
    # Note: The "submission_directory" is also the unique id of a submission!
    submission_directory: str
    chunk_count: int
    # Metadata can be any JSON
    metadata: dict

    # This field should be created by the database
    # created_at: datetime


@app.get("/")
async def root():
    return {"msg": "Hello World"}


@app.get("/submissions")
async def get_submissions():
    """
    Return all submissions stored in the DB.
    """
    # TODO: Actually return the submissions here
    return {"submissions": []}


@app.post("/submissions")
async def post_submissions(submission: Submission):
    # TODO: Insert the submission into the DB here
    return submission


def create_db(filename: str) -> None:
    """
    Create a SQLite database for Opsqueue.

    Does nothing if the file already exists.
    """
    con = sqlite3.connect(filename)
    cur = con.cursor()
    cur.execute("CREATE TABLE chunks(id, url, created_at)")


def main() -> None:
    print("Starting up Opsqueue...")


if __name__ == "main":
    main()


def test_read_main():
    response = client.get("/")
    assert response.status_code == 200
    assert response.json() == {"msg": "Hello World"}
