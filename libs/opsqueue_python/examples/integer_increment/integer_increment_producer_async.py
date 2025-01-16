# It's recommended to just experiment with this in ipython
import logging
import asyncio
from opsqueue.producer import ProducerClient

logging.basicConfig(format="%(levelname)s: %(message)s", level=logging.DEBUG)

client = ProducerClient("localhost:3999", "file:///tmp/opsqueue/integer_increment")

async def main():

    input_iter = range(0, 1_000_0)
    output_iter = await client.async_run_submission(input_iter, chunk_size=1000)
    async for x in output_iter:
        print(x)

    # Now do something with the output:
    # for x in output_iter:
    #    print(x)
    # print(sum(output_iter))

if __name__ == "__main__":
    asyncio.run(main())
