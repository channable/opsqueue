# It's recommended to just experiment with this in ipython
import logging
from opsqueue_producer import Client

logging.basicConfig(format="%(levelname)s: %(message)s", level=logging.INFO)
client = Client("localhost:3999", "file:///tmp/opsqueue/")
# result = client.run_submission_chunks(map(lambda x: str(x).encode(), range(0, 1_000)))
result = client.run_submission(range(0, 100_000), 1000)
for x in result:
    print(x)
