import logging
from datetime import datetime
from itertools import accumulate
from typing import Dict, List, Tuple

from balsam._api.models import BatchJobQuery, EventLogQuery
from balsam.schemas import EventOrdering

logger = logging.getLogger(__name__)


def throughput_report(log_query: EventLogQuery, to_state: str = "JOB_FINISHED") -> Tuple[List[datetime], List[int]]:
    finished_logs = log_query.filter(to_state=to_state).order_by(EventOrdering.timestamp)
    times = list(log.timestamp for log in finished_logs)
    return times, list(range(1, len(times) + 1))


def utilization_report(log_query: EventLogQuery, node_weighting: bool = True) -> Tuple[List[datetime], List[float]]:
    job_events = []
    nodes_by_id: Dict[int, float] = {}
    for log in log_query.filter(to_state="RUNNING"):
        usage_count = log.data["num_nodes"] if node_weighting else 1.0
        nodes_by_id[log.job_id] = usage_count
        job_events.append((log.timestamp, usage_count))
    for log in log_query.filter(from_state="RUNNING"):
        job_events.append((log.timestamp, -1.0 * nodes_by_id.get(log.job_id, 1.0)))

    util_times, util_counts = zip(*sorted(job_events))
    return list(util_times), list(accumulate(util_counts))


def available_nodes(batch_job_query: BatchJobQuery) -> Tuple[List[datetime], List[int]]:
    running: List[Tuple[datetime, int]] = []
    for job in batch_job_query:
        if job.start_time is None or job.end_time is None:
            logger.warning(f"Skipping BatchJob {job.id}: missing start/end times")
            continue
        running.append((job.start_time, job.num_nodes))
        running.append((job.end_time, -1 * job.num_nodes))

    if not running:
        return [], []
    running_nodes_times, running_node_counts = zip(*sorted(running))
    return list(running_nodes_times), list(accumulate(running_node_counts))
