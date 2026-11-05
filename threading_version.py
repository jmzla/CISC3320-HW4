import threading
import queue
import time

BUFFER_SIZE = 3
NUM_PRODUCERS = 2
NUM_CONSUMERS = 2
ITEMS_PER_PRODUCER = 5

buffer = queue.Queue(maxsize=BUFFER_SIZE)


def producer(pid):
    for i in range(ITEMS_PER_PRODUCER):
        item = (pid, i)

        buffer.put(item)
        print("Producer", pid, "produced", item)

        time.sleep(0.1)

    print("Producer", pid, "finished")


def consumer(cid):
    while True:
        item = buffer.get()

        if item is None:
            buffer.task_done()
            print("Consumer", cid, "stopped")
            break

        print("Consumer", cid, "consumed", item)

        time.sleep(0.2)
        buffer.task_done()


def main():
    producers = []
    consumers = []

    for i in range(NUM_CONSUMERS):
        t = threading.Thread(target=consumer, args=(i + 1,))
        consumers.append(t)
        t.start()

    for i in range(NUM_PRODUCERS):
        t = threading.Thread(target=producer, args=(i + 1,))
        producers.append(t)
        t.start()

    for t in producers:
        t.join()

    for i in range(NUM_CONSUMERS):
        buffer.put(None)

    buffer.join()

    for t in consumers:
        t.join()

    print("All producers and consumers finished")
    print("Items left in buffer:", buffer.qsize())


if __name__ == "__main__":
    start_time = time.perf_counter()
    main()
    end_time = time.perf_counter()
    print("Execution Time:", format(end_time - start_time, ".4f"), "seconds")
