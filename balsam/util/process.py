"""
multiprocessing.Process has a hard-coded excepthook that prints exceptions to stderr
We use this to ensure that all exceptions get logged uniformly before bubbling up to stderr
"""
import multiprocessing
import sys

from .log import log_uncaught_exceptions


class Process(multiprocessing.Process):
    def run(self):
        try:
            if hasattr(self, "_run"):
                self._run()
            else:
                super().run()
        except Exception:
            log_uncaught_exceptions(*sys.exc_info())
            raise
