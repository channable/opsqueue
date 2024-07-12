#!/usr/bin/env python
"""
This file implements a super simple consumer for Opsqueue.
"""
import requests

def main():
    while True:
        # Get the next chunk from the queue
        requests.get("http://localhost/chunks")

        # Download the chunk file from GCS

        # Process all operations in the chunk

        # Send a success message back to the queue

if __name__ == "__main__":
    main()