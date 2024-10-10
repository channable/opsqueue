#!/usr/bin/env python
"""
This file implements a super simple producer for Opsqueue.

Every second it inserts a random submission into the queue.
"""

import random
import requests
import time
import uuid

from google.cloud import storage


def upload_to_bucket(path_to_file: str, bucket_name: str, blob_name: str):
    """
    Upload the given file to the given bucket (using the blob_name).
    """

    # Note: This will infer the project and the credentials from the environment.
    storage_client = storage.Client()

    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_filename(path_to_file)

    return blob.public_url


def upload_chunks(test_bucket, blob_name, chunk_count: int):
    """
    Upload the given number of chunks to GCS.
    """
    for i in range(1, chunk_count + 1):
        final_blob_name = blob_name + "/" + str(i)
        # TODO: The 'test_opsfile' is hard-coded here for the moment
        url = upload_to_bucket("test_opsfile", test_bucket, final_blob_name)
        print(f"Uploaded chunk: {i}/{chunk_count}. {url}")


def main():
    # Made up directory path
    submissions_directory = "gs://channable-opsqueue-experimentation/urls_to_download"
    test_bucket = "channable-opsqueue-experimentation"

    while True:
        new_uuid = str(uuid.uuid4())
        submission_directory = submissions_directory + "/" + new_uuid

        # We pick a random number between 1 and 3 as the number of chunks
        chunk_count = random.randint(1, 3)

        # Submit the submission
        data = {
            "submission_directory": submission_directory,
            "chunk_count": chunk_count,
        }

        # We upload the ops_file to GCS here
        blob_name = "urls_to_download/" + new_uuid

        # TODO: The 'test_opsfile' is hard-coded here for the moment
        upload_chunks(test_bucket, blob_name, chunk_count)

        result = requests.post("http://localhost:8000/submissions", json=data)
        print(f"Result: {result}")

        time.sleep(2)


if __name__ == "__main__":
    main()
