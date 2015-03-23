__author__ = 'guilherme'

import threading
import multiprocessing


def repeat(interval, worker_function, args=(), wait=False, iterations=0):

    job = multiprocessing.Process(target=worker_function, args=args)
    job.start()

    if wait:
        job.join()

    if iterations != 1:
        thread = threading.Timer(interval, repeat, [interval, worker_function, args, 0 if iterations == 0 else iterations-1])
        thread.start()