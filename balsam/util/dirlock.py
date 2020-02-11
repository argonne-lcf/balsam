from pathlib import Path
import os
import random
import time
import signal
import threading


class SignalReceived(Exception):
    pass


class DirLock:
    """
    Use as context manager
    with DirLock("/folder/to/lock"):
        ...
    """

    def __init__(self, path, suffix, timeout=5.0, refresh=5.0, expiration=40.0):
        path = Path(path)
        assert path.is_dir()
        self.lock_path = path.joinpath(".DirLock." + suffix)
        self.is_owned_mutex = threading.Lock()
        self.is_owned = False
        self.timeout = timeout
        self.refresh_period = refresh
        self.expiration_seconds = expiration

    def check_stale(self):
        try:
            mtime = self.lock_path.stat().st_mtime
        except OSError:
            return False
        else:
            return time.time() - mtime > self.expiration_seconds

    def lock_held(self):
        return self.lock_path.is_dir() and not self.check_stale()

    def acquire_lock(self):
        acquired = False
        start = time.time()

        def _time_left():
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
            raise TimeoutError(
                f"Failed to acquire {self.lock_path} for {self.timeout} sec"
            )

    def release_lock(self):
        try:
            os.rmdir(self.lock_path)
        except FileNotFoundError:
            pass

    def start_refresher(self):
        self.is_owned = True

        def _do_refresh():
            while True:
                time.sleep(self.refresh_period)
                with self.is_owned_mutex:
                    if self.is_owned:
                        self.lock_path.touch()
                    else:
                        break

        self.refresh_daemon = threading.Thread(target=_do_refresh, daemon=True)
        self.refresh_daemon.start()

    def stop_refresher(self):
        with self.is_owned_mutex:
            self.is_owned = False

    @staticmethod
    def raise_handler(signum, stack):
        raise SignalReceived(f"Killed by signal {signum}")

    def __enter__(self):
        self.term_handler = signal.getsignal(signal.SIGTERM)
        signal.signal(signal.SIGTERM, self.raise_handler)
        self.acquire_lock()
        self.start_refresher()

    def __exit__(self, exc_type, exc_value, traceback):
        self.stop_refresher()
        self.release_lock()
        signal.signal(signal.SIGTERM, self.term_handler)
