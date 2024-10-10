# Popsqueue - an Opsqueue prototype in Python

Run the webserver with: `uvicorn opsqueue:app`.

Run the tests with: `pytest opsqueue.py`

Inspect the DB with: `sqlitebrowser opsqueue.db`

Example request to create a new submission:

```curl
curl --header "Content-Type: application/json" \
  --request POST \
  --data '{"submission_directory":"asdf","chunk_count": 12}' \
  http://localhost:8000/submissions
```

Chunks: `curl --request GET --url "http://127.0.0.1:8000/chunks"`