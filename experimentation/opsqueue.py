#!/usr/bin/env python
import sqlite3

from fastapi import FastAPI
from fastapi.testclient import TestClient

app = FastAPI()
client = TestClient(app)


@app.get("/")
async def root():
    return {"msg": "Hello World"}



def create_db(filename: str) -> None:
    """
    Create a SQLite database for Opsqueue.

    Does nothing if the file already exists.
    """
    con = sqlite3.connect(filename)
    cur = con.cursor()
    cur.execute("CREATE TABLE chunks(id, url)")


def main() -> None:
    print("Starting up Opsqueue...")


if __name__ == "main":
    main()


def test_read_main():
    response = client.get("/")
    assert response.status_code == 200
    assert response.json() == {"msg": "Hello World"}
