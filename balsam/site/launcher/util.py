import logging
import time
from typing import Iterator

logger = logging.getLogger(__name__)


def countdown_timer_min(time_limit_min: int, delay_sec: int) -> Iterator[float]:
    start = time.time()
    next_time = start + delay_sec
    while True:
        now = time.time()
        to_sleep = next_time - now
        if to_sleep <= 0:
            next_time = now + delay_sec
        else:
            time.sleep(to_sleep)
            now += to_sleep
            next_time = now + delay_sec
        elapsed_min = (now - start) / 60.0
        remaining_min = time_limit_min - elapsed_min

        if remaining_min > 0:
            yield remaining_min
        else:
            return
