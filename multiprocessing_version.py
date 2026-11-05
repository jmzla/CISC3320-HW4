import multiprocessing
import time

BUFFER_SIZE = 3
NUM_PRODUCERS = 2
NUM_CONSUMERS = 2
ITEMS_PER_PRODUCER = 5


def producer(pid, buffer):
    for i in range(ITEMS_PER_PRODUCER):
        item = (pid, i)

        buffer.put(item)
        print("Producer", pid, "produced", item, flush=True)

        time.sleep(0.1)

    print("Producer", pid, "finished", flush=True)


def consumer(cid, buffer):
    while True:
        item = buffer.get()

        if item is None:
            print("Consumer", cid, "stopped", flush=True)
            break

        print("Consumer", cid, "consumed", item, flush=True)

        time.sleep(0.2)


def main():
    buffer = multiprocessing.Queue(maxsize=BUFFER_SIZE)

    producers = []
    consumers = []

    for i in range(NUM_CONSUMERS):
        p = multiprocessing.Process(
            target=consumer,
            args=(i + 1, buffer)
        )
        consumers.append(p)
        p.start()

    for i in range(NUM_PRODUCERS):
        p = multiprocessing.Process(
            target=producer,
            args=(i + 1, buffer)
        )
        producers.append(p)
        p.start()

    for p in producers:
        p.join()

    for i in range(NUM_CONSUMERS):
        buffer.put(None)

    for p in consumers:
        p.join()

    print("All producers and consumers finished")
    print("Items left in buffer:", buffer.qsize())

    buffer.close()
    buffer.join_thread()


if __name__ == "__main__":
    start_time = time.perf_counter()
    main()
    end_time = time.perf_counter()
    print("Execution Time:", format(end_time - start_time, ".4f"), "seconds")
