#!/usr/bin/env python
import sqlite3

from fastapi import FastAPI

app = FastAPI()


@app.get("/")
async def root():
    return {"message": "Hello World"}

def main() -> None:
    print("Starting up Opsqueue...")
    con = sqlite3.connect("opsqueue.db")
    cur = con.cursor()
    cur.execute("CREATE TABLE chunks(id, url)")

if __name__ == 'main':
    main()