import logging
from opsqueue_consumer import Client, Strategy
import pickle

logging.basicConfig(format="%(levelname)s: %(message)s", level=logging.DEBUG)


def my_operation(data: int) -> int:
    # print(data)
    return data + 1


client = Client("ws://localhost:3998", "file:///tmp/opsqueue/")
client.run_each_op(my_operation, strategy=Strategy.Newest, serialization_format=pickle)
