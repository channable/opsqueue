import logging
import random
from opsqueue.producer import ProducerClient

logging.basicConfig(format="%(levelname)s: %(message)s", level=logging.INFO)

client = ProducerClient("localhost:3999", "file:///tmp/opsqueue/multi/")

input_iter = range(0, 10_000)

# This user_id is used by the 'multiprocessing_consumer_preferdistinct.py' example consumer
user_id = random.randint(0, 1000)
print(f"Creating submission with user_id: {user_id}")
output_iter = client.run_submission(
    input_iter, chunk_size=10, strategic_metadata={"user_id": user_id}
)

# Now do something with the output:
# for x in output_iter:
#    print(x)
print(sum(output_iter))
