import logging
from opsqueue_consumer import Client, Strategy

logging.basicConfig(format="%(levelname)s: %(message)s", level=logging.INFO)


def my_operation(data: int) -> int:
    return data + 1


client = Client("ws://localhost:3998", "file:///tmp/opsqueue/")
client.run_each_op(my_operation, strategy=Strategy.Newest)
