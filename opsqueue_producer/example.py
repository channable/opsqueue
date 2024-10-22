# It's recommended to just experiment with this in ipython
import logging
from opsqueue_producer import Client

logging.basicConfig(format="%(levelname)s: %(message)s", level=logging.INFO)
client = Client("http://localhost:3999", "file:///tmp/opsqueue/")
result = client.run_submission(map(lambda x: str(x).encode(), range(0, 1_000)))
result
