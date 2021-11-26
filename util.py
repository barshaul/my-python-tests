import time
import numpy as np


def current_milli_time():
    return round(time.time() * 1000)


def get_next_noisy_time(prev_noisy_time=0):
    if prev_noisy_time == 0:
        prev_noisy_time = current_milli_time()
    interval = np.random.uniform(2000, 3001)
    print(interval)
    next_noisy = prev_noisy_time + interval
    print(f'next-intrval={next_noisy-interval}, prev={prev_noisy_time}')
    return round(next_noisy)
