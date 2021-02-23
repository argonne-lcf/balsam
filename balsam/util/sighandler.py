import signal
from threading import Event
from typing import Any


class SigHandler:
    _exit_event = Event()

    def __init__(self) -> None:
        """Registers SIGTERM, SIGINT handlers"""
        signal.signal(signal.SIGTERM, SigHandler._handler)
        signal.signal(signal.SIGINT, SigHandler._handler)

    @staticmethod
    def _handler(signum: int, stack: Any) -> None:
        SigHandler._exit_event.set()

    @staticmethod
    def is_set() -> bool:
        """Return True if triggered; time to exit"""
        return SigHandler._exit_event.is_set()

    @staticmethod
    def wait_until_exit(timeout: float = 1.0) -> bool:
        """Sleep up to timeout seconds. Return True immediately if triggered."""
        return SigHandler._exit_event.wait(timeout=timeout)

    @staticmethod
    def set() -> None:
        """Trigger exit state; time to exit"""
        SigHandler._exit_event.set()
