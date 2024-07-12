#!/usr/bin/env python
"""
This file implements a super simple consumer for Opsqueue.
"""
import os
import requests
import time

from google.cloud import storage

DIR_PATH = os.path.dirname(os.path.realpath(__file__))


def download_from_bucket(blob_name: str, local_file_path: str):
    """
    Download the given blob from GCS.
    """
    # Hard-coded, for now.
    bucket_name = "channable-opsqueue-experimentation"

    # Note: This will infer the project and the credentials from the environment.
    storage_client = storage.Client()

    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(blob_name)

    full_file_path = DIR_PATH + "/chunks/" + local_file_path
    print(f"Full file path: {full_file_path}")
    blob.download_to_filename(full_file_path)

    return blob.public_url


def ops_generator(blob_name: str):
    """
    Each line in the file must be one operation.
    """
    with open(blob_name, "r") as f:
        for line in f:
            yield line.strip()


def process(op):
    """
    Process one operation.

    In this simple example we send one HTTP request for each operation and print the return status code.
    """
    result = requests.get(op)
    print(result.status_code)

    return result


def main():
    """
    Run an infinite loop that gets a chunk from the queue, processes it, and then gets the next one.
    """
    while True:
        # Get the next chunk from the queue
        chunks = requests.get("http://localhost:8000/chunks")

        # We simply pick the first chunk, for now. TODO: Pick a chunk according to a strategy.
        chunk = chunks.json()["chunks"][0]

        # One chunk looks like this:
        # {"submission_directory":"gs://channable-opsqueue-experimentation/urls_to_download/cc364963-d9ca-406d-8ee5-be71daf415fe","chunk_id":2}

        # HACK: It's ugly that we need to modify the URL here. Does the google.cloud library have no option to download a gs:// URL directly?
        blob_name = (
            chunk["submission_directory"].replace(
                "gs://channable-opsqueue-experimentation/", ""
            )
            + "/"
            + str(chunk["chunk_id"])
        )

        # TODO: This is ugly
        local_file_path = (
            chunk["submission_directory"].split("/")[-1] + "-" + str(chunk["chunk_id"])
        )

        # We keep the same file name locally
        _public_url = download_from_bucket(blob_name, local_file_path=local_file_path)

        # Process all operations in the chunk
        failed_ops = []
        full_file_path = DIR_PATH + "/chunks/" + local_file_path
        for op in ops_generator(full_file_path):
            try:
                process(op)
            except Exception as e:
                print(f"Failed to process op: {op}. Exception: {e}")

        # Send a success message back to the queue
        if failed_ops:
            print(f"The following operations have failed: {failed_ops}")
            # TODO: We should retry the whole chunk here since at least one operation failed
        else:
            # TODO: Report success back to Opsqueue (we still need an endpoint for that)
            pass

        # Sleep for 2 seconds to not spam too much
        time.sleep(2)


if __name__ == "__main__":
    main()
