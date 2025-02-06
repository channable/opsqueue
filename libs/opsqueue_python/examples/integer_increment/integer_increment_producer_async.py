# It's recommended to just experiment with this in ipython
import logging
import asyncio
from opsqueue.producer import ProducerClient

logging.basicConfig(format="%(levelname)s: %(message)s", level=logging.INFO)

client = ProducerClient("localhost:3999", "file:///tmp/opsqueue/integer_increment")


async def main(top: int = 1_000) -> int:
    input_iter = range(0, top)
    output_iter = await client.async_run_submission(input_iter, chunk_size=1000)
    logging.info(f"Submission for {top} done!")

    res = 0
    async for x in output_iter:
        res += x

    logging.info(f"Finished summing {top}: {res}")
    return res


async def multi_main() -> None:
    res = await asyncio.gather(
        main(100),
        main(1000),
        main(10_000),
        main(20_000),
        main(12_345),
    )
    print(res)


if __name__ == "__main__":
    asyncio.run(multi_main())
