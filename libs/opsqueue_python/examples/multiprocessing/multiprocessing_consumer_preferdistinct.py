# Example of using the 'PreferDistinct' strategy
# This ensures that multiple consumers will only work on the same submission
# if there truly is no other work to do.
# In other words: If submissions of equal size were started by different users,
# we expect them to finish roughly at the same time.
import logging
import multiprocessing
from opsqueue.consumer import ConsumerClient, Strategy


def run_a_consumer(id: int) -> None:
    logging.basicConfig(
        format=f"Consumer {id} - %(levelname)s: %(message)s", level=logging.INFO
    )

    def my_operation(data: int) -> int:
        # time.sleep(0.01)
        return data + 1

    client = ConsumerClient("localhost:3999", "file:///tmp/opsqueue/multi/")
    strategy = Strategy.PreferDistinct(meta_key="user_id", underlying=Strategy.Random())
    client.run_each_op(my_operation, strategy=strategy)


def main() -> None:
    n_consumers = 4
    processes = [
        multiprocessing.Process(target=run_a_consumer, args=(id,))
        for id in range(n_consumers)
    ]
    for p in processes:
        p.start()
    for p in processes:
        p.join()


if __name__ == "__main__":
    main()
