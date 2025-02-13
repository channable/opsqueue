# It's recommended to just experiment with this in ipython
import logging
from opsqueue.producer import ProducerClient

logging.basicConfig(format="%(levelname)s: %(message)s", level=logging.DEBUG)

client = ProducerClient("localhost:3999", "file:///tmp/opsqueue/integer_increment")

input_iter = range(0, 1_000_000)
output_iter = client.run_submission(input_iter, chunk_size=1000)

# Now do something with the output:
# for x in output_iter:
#    print(x)
print(sum(output_iter))
