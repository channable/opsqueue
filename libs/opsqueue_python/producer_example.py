# It's recommended to just experiment with this in ipython
import logging
from opsqueue.producer import ProducerClient

logging.basicConfig(format="%(levelname)s: %(message)s", level=logging.INFO)

client = ProducerClient("localhost:3999", "file:///tmp/opsqueue/")

input_iter = range(0, 100_00)
output_iter = client.run_submission(input_iter, chunk_size=100)

# Now do something with the output:
# for x in output_iter:
#    print(x)
print(sum(output_iter))
