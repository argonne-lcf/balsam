import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Tuple

from sqlalchemy import Float, func, orm
from sqlalchemy.sql import Select, cast, select, update

from balsam import schemas
from balsam.server import ValidationError, models
from balsam.server.routers.filters import SessionQuery

from .jobs import do_update_jobs, owned_job_selector, select_jobs_for_update

logger = logging.getLogger(__name__)

SESSION_EXPIRE_PERIOD = timedelta(minutes=5)
SESSION_SWEEP_PERIOD = timedelta(minutes=1)
latest_sweep_time: Dict[int, datetime] = {}
Session = orm.Session
Query = orm.Query


def owned_session_query(db: Session, owner: schemas.UserOut) -> "Query[models.Session]":
    return db.query(models.Session).join(models.Site).filter(models.Site.owner_id == owner.id)  # type: ignore


def fetch(db: Session, owner: schemas.UserOut, filterset: SessionQuery) -> Tuple[int, List[models.Session]]:
    qs = owned_session_query(db, owner)
    result = filterset.apply_filters(qs).order_by(models.Session.id).all()
    count = len(result)
    return count, result


def _timeout_jobs(db: Session, session: models.Session) -> None:
    zombies = (
        select((models.Job.__table__,))
        .where(models.Job.session_id == session.id)
        .where(models.Job.state == "RUNNING")
    )
    update_jobs, transfer_items_by_jobid = select_jobs_for_update(db, zombies)
    timeout_dat = {
        "state": "RUN_TIMEOUT",
        "state_data": {"message": "Session expired: job was stuck in RUNNING state"},
    }
    if update_jobs:
        do_update_jobs(
            db,
            update_jobs,
            transfer_items_by_jobid,
            patch_dicts={job.id: timeout_dat.copy() for job in update_jobs},
        )
        logger.info(f"Timed out {len(update_jobs)} running jobs in expired session {session.id}")


def _clear_stale_sessions(db: Session, owner: schemas.UserOut) -> None:
    now = datetime.utcnow()

    last_sweep = latest_sweep_time.get(owner.id)
    if last_sweep is not None and now < last_sweep + SESSION_SWEEP_PERIOD:
        return

    latest_sweep_time[owner.id] = now
    expiry_time = now - SESSION_EXPIRE_PERIOD

    sess_ids = []

    expired_sessions = owned_session_query(db, owner).filter(models.Session.heartbeat <= expiry_time).all()
    for sess in expired_sessions:
        logger.info(f"Session {sess.id} expired: last heartbeat was {sess.heartbeat}; expiry_time is {expiry_time}")
        sess_ids.append(sess.id)

    finished_launcher_sessions = (
        owned_session_query(db, owner)  # type: ignore
        .join(models.BatchJob, models.BatchJob.id == models.Session.batch_job_id)
        .filter(models.BatchJob.state == "finished")
        .all()
    )
    for sess in finished_launcher_sessions:
        if sess.id not in sess_ids:
            logger.info(f"Session {sess.id} expired: the assosciated BatchJob state is finished.")
            expired_sessions.append(sess)
            sess_ids.append(sess.id)

    for session in expired_sessions:
        _timeout_jobs(db, session)

    db.query(models.Session).filter(models.Session.id.in_(sess_ids)).delete(synchronize_session=False)
    db.flush()


def create(db: Session, owner: schemas.UserOut, session: schemas.SessionCreate) -> models.Session:
    _clear_stale_sessions(db, owner)

    site_id = (
        db.query(models.Site.id).filter(models.Site.owner_id == owner.id, models.Site.id == session.site_id).scalar()  # type: ignore
    )
    if site_id is None:
        raise ValidationError(f"No site with ID {session.site_id}")
    if session.batch_job_id is not None:
        batch_job_id = (
            db.query(models.BatchJob.id)
            .filter(models.BatchJob.site_id == site_id)
            .filter(models.BatchJob.id == session.batch_job_id)
        ).scalar()  # type: ignore
        if batch_job_id is None:
            raise ValidationError(f"No batch_job with id {session.batch_job_id}")

    created_session = models.Session(**session.dict())
    db.add(created_session)
    db.flush()
    return created_session


def _acquire_jobs(db: orm.Session, job_q: Select, session: models.Session) -> List[Dict[str, Any]]:
    acquired_jobs = [{str(key): value for key, value in job.items()} for job in db.execute(job_q).mappings()]
    acquired_ids = [job["id"] for job in acquired_jobs]
    # logger.info(f"*** in _acquire_jobs acquired_ids={acquired_ids}")

    stmt = update(models.Job.__table__).where(models.Job.id.in_(acquired_ids)).values(session_id=session.id)

    # Do not overwrite job.batch_job_id with a Session that has no batch_job_id:
    if session.batch_job_id is not None:
        stmt = stmt.values(batch_job_id=session.batch_job_id)
        for job in acquired_jobs:
            job["batch_job_id"] = session.batch_job_id
            job["session_id"] = session.id

    db.execute(stmt)
    db.flush()
    logger.debug(f"Acquired {len(acquired_jobs)} jobs")
    return acquired_jobs


def _footprint_func_nodes() -> Any:
    footprint = cast(models.Job.num_nodes, Float) / cast(models.Job.node_packing_count, Float)
    return (
        func.sum(footprint)
        .over(
            order_by=(
                models.Job.num_nodes.asc(),
                models.Job.node_packing_count.desc(),
                models.Job.wall_time_min.desc(),
                models.Job.id.asc(),
            )
        )
        .label("aggregate_footprint")
    )


def _footprint_func_walltime() -> Any:
    footprint = cast(models.Job.num_nodes, Float) / cast(models.Job.node_packing_count, Float)
    return (
        func.sum(footprint)
        .over(
            order_by=(
                models.Job.wall_time_min.desc(),
                models.Job.num_nodes.desc(),
                models.Job.node_packing_count.desc(),
                models.Job.id.asc(),
            )
        )
        .label("aggregate_footprint")
    )


def acquire(
    db: Session, owner: schemas.UserOut, session_id: int, spec: schemas.SessionAcquire
) -> List[Dict[str, Any]]:
    session = (owned_session_query(db, owner).filter(models.Session.id == session_id)).one()
    session.heartbeat = datetime.utcnow()

    logger.debug(f"Acquire: filtering for jobs with states: {spec.states}")

    # Select unlocked jobs at this Site matching the state / tags criteria
    job_q = (
        owned_job_selector(owner)
        .where(models.App.site_id == session.site_id)
        .where(models.Job.session_id.is_(None))  # type: ignore
        .where(models.Job.state.in_(spec.states))
        .where(models.Job.tags.contains(spec.filter_tags))  # type: ignore
    )

    if spec.app_ids:
        job_q = job_q.where(models.Job.app_id.in_(spec.app_ids))

    if spec.max_wall_time_min:
        job_q = job_q.where(models.Job.wall_time_min <= spec.max_wall_time_min)  # By time

    if spec.max_nodes_per_job:
        job_q = job_q.where(models.Job.num_nodes <= spec.max_nodes_per_job)

    if spec.serial_only:
        job_q = job_q.where(models.Job.num_nodes == 1).where(models.Job.ranks_per_node == 1)

    # This is the branch taken by Processing and Serial Mode Launcher:
    if spec.max_aggregate_nodes is None:
        # NO footprint calculation; NO ordering is needed!
        job_q = job_q.limit(spec.max_num_jobs).with_for_update(of=models.Job.__table__, skip_locked=True)
        return _acquire_jobs(db, job_q, session)

    # MPI Mode Launcher will take this path:
    # logger.info(f"*** In session.acquire: spec.sort_by = {spec.sort_by}")
    if spec.sort_by == "long_large_first":
        lock_ids_q = (
            job_q.with_only_columns([models.Job.id])
            .order_by(
                models.Job.wall_time_min.desc(),
                models.Job.num_nodes.desc(),
                models.Job.node_packing_count.desc(),
            )
            .limit(spec.max_num_jobs)
            .with_for_update(of=models.Job.__table__, skip_locked=True)
        )
    else:
        lock_ids_q = (
            job_q.with_only_columns([models.Job.id])
            .order_by(
                models.Job.num_nodes.asc(),
                models.Job.node_packing_count.desc(),
                models.Job.wall_time_min.desc(),
            )
            .limit(spec.max_num_jobs)
            .with_for_update(of=models.Job.__table__, skip_locked=True)
        )

    locked_ids = db.execute(lock_ids_q).scalars().all()
    # logger.info(f"*** locked_ids: {locked_ids}")
    if spec.sort_by == "long_large_first":
        subq = select(models.Job.__table__, _footprint_func_walltime()).where(models.Job.id.in_(locked_ids)).subquery()  # type: ignore
    else:
        subq = select(models.Job.__table__, _footprint_func_nodes()).where(models.Job.id.in_(locked_ids)).subquery()  # type: ignore

    # logger.info(f"*** max_aggregate_nodes: {spec.max_aggregate_nodes}")
    cols = [c for c in subq.c if c.name not in ["aggregate_footprint", "session_id"]]
    job_q = select(cols).where(subq.c.aggregate_footprint <= spec.max_aggregate_nodes)

    return _acquire_jobs(db, job_q, session)


def tick(db: Session, owner: schemas.UserOut, session_id: int) -> datetime:
    in_db = owned_session_query(db, owner).filter(models.Session.id == session_id).one()
    ts = datetime.utcnow()
    in_db.heartbeat = ts
    db.flush()

    # Clear after updating heartbeat, to avoid clearing self
    _clear_stale_sessions(db, owner)
    return ts


def delete(db: Session, owner: schemas.UserOut, session_id: int) -> None:
    qs = owned_session_query(db, owner).filter(models.Session.id == session_id)
    session = qs.one()

    _timeout_jobs(db, session)

    db.query(models.Session).filter(models.Session.id == session_id).delete(synchronize_session=False)
    db.flush()
