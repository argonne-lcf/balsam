import logging
import time
from typing import Iterator
import dill  # type: ignore
from six import reraise
from tblib import Traceback  # type: ignore
from types import TracebackType

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


# Implementation of RemoteExceptionWrapper from parsl.apps.errors
# https://github.com/Parsl/parsl/blob/master/parsl/app/errors.py
class RemoteExceptionWrapper:
    def __init__(self, e_type: type, e_value: Exception, traceback: TracebackType) -> None:

        self.e_type = dill.dumps(e_type)
        self.e_value = dill.dumps(e_value)
        self.e_traceback = Traceback(traceback)

    def reraise(self) -> None:

        t = dill.loads(self.e_type)

        # the type is logged here before deserialising v and tb
        # because occasionally there are problems deserialising the
        # value (see #785, #548) and the fix is related to the
        # specific exception type.
        logger.debug("Reraising exception of type {}".format(t))

        v = dill.loads(self.e_value)
        tb = self.e_traceback.as_traceback()

        reraise(t, v, tb)
