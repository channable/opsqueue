#!/usr/bin/env python
"""
This file implements a super simple producer for Opsqueue.

Every second it inserts a random submission into the queue.
"""

import random
import requests
import time
import uuid


def main():
    # Made up directory path
    submissions_directory = "gs://opsqueue_test_bucket/urls_to_download"

    while True:
        new_uuid = uuid.uuid4()
        submission_directory = submissions_directory + "/" + str(new_uuid)

        # We pick a random number between 1 and 9 as the number of chunks
        chunk_count = random.randint(1, 9)

        # Submit the submission
        data = {
            "submission_directory": submission_directory,
            "chunk_count": chunk_count,
        }

        result = requests.post("http://localhost:8000/submissions", json=data)
        print(f"Result: {result}")

        time.sleep(2)


if __name__ == "__main__":
    main()
