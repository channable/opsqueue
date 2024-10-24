# It's recommended to just experiment with this in ipython
import logging
from opsqueue_producer import Client

logging.basicConfig(format="%(levelname)s: %(message)s", level=logging.INFO)

client = Client("localhost:3999", "file:///tmp/opsqueue/")

input_iter = range(0, 100_000)
output_iter = client.run_submission(input_iter, chunk_size=100)

# Now do something with the output:
for x in output_iter:
    print(x)
