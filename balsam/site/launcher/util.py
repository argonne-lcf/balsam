import time
import logging

logger = logging.getLogger(__name__)


def countdown_timer_min(time_limit_min, delay_sec):
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


class SectionTimer:
    _sections = {}
    total_elapsed = 0.0

    def __init__(self, name):
        self.name = name
        if name not in SectionTimer._sections:
            SectionTimer._sections[name] = []

    def __enter__(self):
        self.t0 = time.perf_counter()

    def __exit__(self, exc_type, exc_val, exc_tb):
        elapsed = time.perf_counter() - self.t0
        SectionTimer._sections[self.name].append(elapsed)
        SectionTimer.total_elapsed += elapsed
        if SectionTimer.total_elapsed > 300:
            self.report()

    @staticmethod
    def report():
        result = (
            f'{"Section":24} {"MinTime":8} {"MaxTime":8} {"AvgTime":8} {"PctTime":5}\n'
        )
        total_t = sum(sum(times) for times in SectionTimer._sections.values())
        for sec, times in SectionTimer._sections.items():
            min_t = min(times)
            max_t = max(times)
            avg_t = sum(times) / len(times)
            percent_t = 100 * sum(times) / total_t
            result += (
                f"{sec:24} {min_t:8.3f} {max_t:8.3f} {avg_t:8.3f} {percent_t:5.1f}%\n"
            )
        SectionTimer._sections = {}
        SectionTimer.total_elapsed = 0.0
        logger.info("\n" + result)
