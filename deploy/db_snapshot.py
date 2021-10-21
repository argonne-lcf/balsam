from pathlib import Path
import subprocess
from datetime import datetime, timedelta
from time import sleep
import logging
import logging.handlers

LOG_PATH = "/home/msalim/db-backups/backup.log"
BACKUP_DIR = "/home/msalim/db-backups"
BACKUP_DESTINATION = "homes.cels.anl.gov:/nfs/gce/projects/balsam"
DUMP_CMD = "docker exec postgres pg_dump -U postgres balsam"
BACKUP_INTERVAL = timedelta(hours=12)
MAX_OLD_BACKUPS = 64


logger = logging.getLogger()
handler = logging.handlers.RotatingFileHandler(LOG_PATH, maxBytes=int(32 * 1e6), backupCount=1)
logger.addHandler(handler)
logger.setLevel(logging.INFO)


def run_backup() -> None:
    backup_dir = Path(BACKUP_DIR).resolve()
    if not backup_dir.is_dir():
        raise RuntimeError(f"BACKUP_DIR {backup_dir} does not exist")

    fname = backup_dir / f"{datetime.now().isoformat()}.sql"
    backup_pattern = "????-??-??T??:??:??.??????.sql"

    with open(fname, "w") as fp:
        subprocess.run(DUMP_CMD, stdout=fp, encoding="utf-8", check=True, shell=True)
    logger.info(f"Created DB backup: {fname}")

    all_backups = sorted(backup_dir.glob(backup_pattern), key=lambda p: p.stat().st_ctime)
    num_to_discard = max(0, len(all_backups) - MAX_OLD_BACKUPS)
    delete_paths = all_backups[:num_to_discard]
    for path in delete_paths:
        path.unlink(missing_ok=True)
        logger.info(f"Deleted old DB backup: {path}")

    subprocess.run(f"rsync -avz {backup_dir}/ {BACKUP_DESTINATION}", shell=True)
    logger.info(f"Completed rsync to {BACKUP_DESTINATION}")


while True:
    try:
        run_backup()
    except Exception as exc:
        logger.exception(f"An error occured in run_backup: {exc}")
        raise
    sleep(BACKUP_INTERVAL.total_seconds())
