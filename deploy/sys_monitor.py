import psutil  # type: ignore
from typing import Dict, Any
from datetime import datetime
import time
import json
import logging
import logging.handlers

LOG_PATH = "/home/msalim/sys-monitoring/data.log"

logger = logging.getLogger()
handler = logging.handlers.RotatingFileHandler(LOG_PATH, maxBytes=int(32 * 1e6), backupCount=3)
logger.addHandler(handler)
logger.setLevel(logging.INFO)


def snapshot() -> Dict[str, Any]:
    cpu_percent = psutil.cpu_percent()
    load_tup = psutil.getloadavg()
    now = datetime.now()
    vm = psutil.virtual_memory()
    du = psutil.disk_usage("/")
    io_counters = psutil.disk_io_counters()
    net_counters = psutil.net_io_counters()

    return {
        "time": now.isoformat(),
        "cpu_percent": cpu_percent,
        "load_1m": load_tup[0],
        "load_5m": load_tup[1],
        "load_15m": load_tup[2],
        "mem_avail_mb": vm.available / 1.0e6,
        "mem_total_mb": vm.total / 1.0e6,
        "disk_mb_used": int(du.used / 1.0e6),
        "disk_percent_used": du.percent,
        "disk_reads": io_counters.read_count,
        "disk_writes": io_counters.write_count,
        "disk_read_mb": io_counters.read_bytes / 1.0e6,
        "disk_write_mb": io_counters.write_bytes / 1.0e6,
        "net_mb_sent": net_counters.bytes_sent / 1.0e6,
        "net_mb_recv": net_counters.bytes_recv / 1.0e6,
    }


snapshot()
time.sleep(1.0)
while True:
    logger.info(json.dumps(snapshot()))
    time.sleep(30)
