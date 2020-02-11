# flake8: noqa
from collections import defaultdict, Counter
import re
import time
import numpy as np
from datetime import datetime, timedelta
from django.utils import timezone
from . import TIME_FMT
from . import logger
from .exceptions import *


def utilization_report(qs):
    time_data = process_job_times(qs)
    start_times = time_data.get("RUNNING", [])
    end_times = []
    for state in ["RUN_DONE", "RUN_ERROR", "RUN_TIMEOUT"]:
        end_times.extend(time_data.get(state, []))

    startCounts = Counter(start_times)
    endCounts = Counter(end_times)
    for t in endCounts:
        endCounts[t] *= -1
    merged = sorted(
        list(startCounts.items()) + list(endCounts.items()), key=lambda x: x[0]
    )
    counts = np.fromiter((x[1] for x in merged), dtype=np.int)

    times = [x[0] for x in merged]
    running = np.cumsum(counts)
    return (times, running)


def throughput_report(qs):
    time_data = process_job_times(qs)
    done_times = time_data.get("RUN_DONE", [])
    doneCounts = sorted(list(Counter(done_times).items()), key=lambda x: x[0])
    times = [x[0] for x in doneCounts]
    counts = np.cumsum(np.fromiter((x[1] for x in doneCounts), dtype=np.int))
    return (times, counts)


def error_report(qs):
    time_data = process_job_times(qs)
    err_times = time_data.get("RUN_ERROR", [])
    if not err_times:
        return
    time0 = min(err_times)
    err_seconds = np.array([(t - time0).total_seconds() for t in err_times])
    hmin, hmax = 0, max(err_seconds)
    bins = np.arange(hmin, hmax + 60, 60)
    times = [time0 + timedelta(seconds=s) for s in bins]
    hist, _ = np.histogram(err_seconds, bins=bins, density=False)
    assert len(times) == len(hist) + 1
    return times, hist
