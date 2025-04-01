import logging
import multiprocessing
from opsqueue.producer import ProducerClient

logging.basicConfig(
    format="%(levelname)s - %(asctime)s: %(message)s", level=logging.INFO
)


def run_a_producer(id: int) -> None:
    client = ProducerClient("localhost:3999", "file:///tmp/opsqueue/multi/")
    input_iter = range(0, 2_000)
    output_iter = client.run_submission(
        input_iter, chunk_size=10, strategic_metadata={"user_id": id}
    )
    logging.info(sum(output_iter))


def main() -> None:
    n_producers = 4
    print(
        f"Starting {n_producers} producers... When `multiprocessing_consumer_preferdistinct.py` is used, expecting all submissions to finish roughly at the same time"
    )
    processes = [
        multiprocessing.Process(target=run_a_producer, args=(id,))
        for id in range(n_producers)
    ]
    for p in processes:
        p.start()
    for p in processes:
        p.join()


if __name__ == "__main__":
    main()
