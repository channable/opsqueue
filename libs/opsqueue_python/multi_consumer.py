import logging
import time
import multiprocessing
from opsqueue.consumer import ConsumerClient, Strategy



def run_a_consumer(id: int) -> None:
    logging.basicConfig(format=f"Consumer {id} - %(levelname)s: %(message)s", level=logging.INFO)

    def my_operation(data: int) -> int:
        # time.sleep(0.01)
        return data + 1

    client = ConsumerClient("ws://localhost:3999", "file:///tmp/opsqueue/")
    client.run_each_op(my_operation, strategy=Strategy.Random)

def main():
    n_consumers = 1024
    processes = [multiprocessing.Process(target=run_a_consumer, args=(id,)) for id in range(n_consumers)]
    for p in processes:
        p.start()
    for p in processes:
        p.join()

if __name__ == '__main__':
    main()
