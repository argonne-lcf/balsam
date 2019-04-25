import os
import sys
import random
import time

class InfoLock:
    EXPIRATION_SECONDS = 20

    def __init__(self, path):
        assert os.path.isdir(path)
        self.lock_path = os.path.join(path, '.infolock')

    def check_stale(self):
        try: mtime = os.path.getmtime(self.lock_path)
        except OSError: return False
        else: return (time.time() - mtime > self.EXPIRATION_SECONDS)

    def acquire_lock(self, timeout=40.0):
        acquired = False
        start = time.time()
        newline = False
        def _time_left():
            if timeout is None: return True
            return time.time() - start < timeout

        while not acquired and _time_left():
            try: os.mkdir(self.lock_path)
            except OSError:
                time.sleep(1.0 + random.uniform(0, 0.5))
                print(".", end='', flush=True)
                newline = True
                if self.check_stale():
                    try: os.rmdir(self.lock_path)
                    except FileNotFoundError: pass
            else: 
                acquired = True
                if newline: print("")
        if not acquired:
            raise TimeoutError(f'Failed to acquire {self.lock_path} for {timeout} sec')

    def release_lock(self):
        try: os.rmdir(self.lock_path)
        except FileNotFoundError: pass
            
    def __enter__(self):
        self.acquire_lock()
    
    def __exit__(self, exc_type, exc_value, traceback):
        self.release_lock()
