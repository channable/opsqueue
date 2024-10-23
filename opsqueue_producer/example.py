# It's recommended to just experiment with this in ipython
import logging
from opsqueue_producer import Client, json_as_bytes

logging.basicConfig(format="%(levelname)s: %(message)s", level=logging.INFO)
client = Client("localhost:3999", "file:///tmp/opsqueue/")
# result = client.run_submission_chunks(map(lambda x: str(x).encode(), range(0, 1_000)))
result = client.run_submission(range(0, 1_000_000), chunk_size = 10000)
for x in result:
    print(x)
