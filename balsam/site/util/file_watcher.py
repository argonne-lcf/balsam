import time
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, Tuple


def _snapshot_files(directory: str, pattern: str) -> Iterable[Tuple[Path, float]]:
    seen_files = set()
    for file in Path(directory).glob(pattern):
        if file in seen_files:
            continue
        try:
            mtime = file.stat().st_mtime
        except OSError:
            continue
        seen_files.add(file)
        yield file, mtime


def watcher(callback: Callable[[str], Any], directory: str, glob_pattern: str, sleep_time: float) -> None:
    """
    Invokes callback(path) for every modified file
    """
    mtimes: Dict[Path, float] = {}
    while True:
        time.sleep(sleep_time)
        for filepath, mtime in _snapshot_files(directory, glob_pattern):
            old_time = mtimes.get(filepath)
            mtimes[filepath] = mtime
            if old_time is None or mtime > old_time:
                callback(filepath.as_posix())
