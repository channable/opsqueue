import logging
from opsqueue.consumer import ConsumerClient, Strategy
import time

logging.basicConfig(format="%(levelname)s: %(message)s", level=logging.INFO)


def my_operation(data: int) -> int:
    # time.sleep(0.1)
    return data + 1


client = ConsumerClient("ws://localhost:3999", "file:///tmp/opsqueue/")
client.run_each_op(my_operation, strategy=Strategy.Oldest)
