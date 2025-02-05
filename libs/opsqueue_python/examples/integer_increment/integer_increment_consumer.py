import logging
from opsqueue.consumer import ConsumerClient, Strategy

logging.basicConfig(format="%(levelname)s: %(message)s", level=logging.INFO)


def my_operation(data: int) -> int:
    # time.sleep(0.1)
    return data + 1


client = ConsumerClient("localhost:3999", "file:///tmp/opsqueue/integer_increment")
client.run_each_op(my_operation, strategy=Strategy.Random())
