import logging
from opsqueue_consumer import Client, Strategy, Chunk


def myfun(chunk: Chunk) -> bytes:
    import time
    time.sleep(0.1) # We simulate that this takes a while
    if chunk.input_content is None:
        return b"Hello world!"
    else:
        return chunk.input_content + b"Was altered!"


logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.DEBUG)
client = Client("ws://localhost:3998")
client.run_per_chunk(Strategy.Oldest, myfun)
