import os
import random
import signal
import threading
import time
from pathlib import Path
from typing import Any, Union


class SignalReceived(Exception):
    pass


class DirLock:
    """
    Use as context manager
    with DirLock("/folder/to/lock"):
        ...
    """

    def __init__(
        self,
        path: Union[str, Path],
        suffix: str,
        timeout: float = 5.0,
        refresh: float = 5.0,
        expiration: float = 40.0,
    ) -> None:
        path = Path(path)
        assert path.is_dir()
        self.lock_path = path.joinpath(".DirLock." + suffix)
        self.is_owned_mutex = threading.Lock()
        self.is_owned = False
        self.timeout = timeout
        self.refresh_period = refresh
        self.expiration_seconds = expiration

    def check_stale(self) -> bool:
        try:
            mtime = self.lock_path.stat().st_mtime
        except OSError:
            return False
        else:
            return time.time() - mtime > self.expiration_seconds

    def lock_held(self) -> bool:
        return self.lock_path.is_dir() and not self.check_stale()

    def acquire_lock(self) -> None:
        acquired = False
        start = time.time()

        def _time_left() -> bool:
            if self.timeout is None:
                return True
            return time.time() - start < self.timeout

        while not acquired and _time_left():
            try:
                os.mkdir(self.lock_path)
            except FileExistsError:
                time.sleep(1.0 + random.uniform(0, 0.5))
                if self.check_stale():
                    try:
                        os.rmdir(self.lock_path)
                    except FileNotFoundError:
                        pass
            else:
                acquired = True
        if not acquired:
            raise TimeoutError(f"Failed to acquire {self.lock_path} for {self.timeout} sec")

    def release_lock(self) -> None:
        try:
            os.rmdir(self.lock_path)
        except FileNotFoundError:
            pass

    def start_refresher(self) -> None:
        self.is_owned = True

        def _do_refresh() -> None:
            while True:
                time.sleep(self.refresh_period)
                with self.is_owned_mutex:
                    if self.is_owned:
                        self.lock_path.touch()
                    else:
                        break

        self.refresh_daemon = threading.Thread(target=_do_refresh, daemon=True)
        self.refresh_daemon.start()

    def stop_refresher(self) -> None:
        with self.is_owned_mutex:
            self.is_owned = False

    @staticmethod
    def raise_handler(signum: int, stack: Any) -> None:
        raise SignalReceived(f"Killed by signal {signum}")

    def __enter__(self) -> None:
        self.term_handler = signal.getsignal(signal.SIGTERM)
        signal.signal(signal.SIGTERM, self.raise_handler)
        self.acquire_lock()
        self.start_refresher()

    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> None:
        self.stop_refresher()
        self.release_lock()
        signal.signal(signal.SIGTERM, self.term_handler)
