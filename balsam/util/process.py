"""
multiprocessing.Process has a hard-coded excepthook that prints exceptions to stderr
We use this to ensure that all exceptions get logged uniformly before bubbling up to stderr
"""
import multiprocessing
import sys

from .log import log_uncaught_exceptions


class Process(multiprocessing.Process):
    def _run(self) -> None:
        raise NotImplementedError

    def run(self) -> None:
        try:
            self._run()
        except NotImplementedError:
            try:
                super().run()
            except Exception:
                log_uncaught_exceptions(*sys.exc_info())
                raise
        except Exception:
            log_uncaught_exceptions(*sys.exc_info())
            raise
