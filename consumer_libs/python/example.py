from opsqueue_consumer import Client, Strategy, Chunk

def myfun(chunk: Chunk) -> bytes:
    if chunk.input_content is None:
        return b"Hello world!"
    else:
        return chunk.input_content + b"Was altered!"

client = Client("ws://localhost:3998")
client.run_per_chunk(Strategy.Oldest, myfun)
