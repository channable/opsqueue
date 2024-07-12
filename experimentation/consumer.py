#!/usr/bin/env python
"""
This file implements a super simple consumer for Opsqueue.
"""
import requests
import time


def ops_generator():
    # TODO: The opsfile here is hard-coded (instead of downloaded from GCS)
    with open("test_opsfile", "r") as f:
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
        _chunks = requests.get("http://localhost:8000/chunks")

        # Download the chunk file from GCS
        # TODO: We currently skip this part and just use a local test file with operations

        # Process all operations in the chunk
        failed_ops = []
        for op in ops_generator():
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
