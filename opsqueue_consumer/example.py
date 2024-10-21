import logging
from opsqueue_consumer import Client, Strategy, Chunk


def myfun(chunk: Chunk) -> bytes:
    # time.sleep(1) # We simulate that this takes a while
    # return 1 / 0
    if chunk.input_content is None:
        return b"Hello world!"
    else:
        return chunk.input_content + b"Was altered!"


logging.basicConfig(format="%(levelname)s: %(message)s", level=logging.INFO)
client = Client("ws://localhost:3998", "file:///tmp/opsqueue/")
client.run_per_chunk(Strategy.Newest, myfun)
