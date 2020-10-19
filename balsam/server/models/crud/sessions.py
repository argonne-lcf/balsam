from datetime import datetime, timedelta
from balsam.server import models, ValidationError
from balsam import schemas
from .jobs import update_states_by_query
from sqlalchemy import orm

SESSION_EXPIRE_PERIOD = timedelta(minutes=5)
SESSION_SWEEP_PERIOD = timedelta(minutes=3)
_sweep_time = None


def owned_session_query(db, owner):
    return (
        db.query(models.Session)
        .join(models.Site)
        .filter(models.Site.owner_id == owner.id)
    )


def fetch(db, owner):
    qs = owned_session_query(db, owner).all()
    count = len(qs)
    return count, qs


def _clear_stale_sessions(db, owner):
    global _sweep_time
    now = datetime.utcnow()
    if _sweep_time is not None and now < _sweep_time + SESSION_SWEEP_PERIOD:
        return [], []
    _sweep_time = now
    expiry_time = now - SESSION_EXPIRE_PERIOD
    expired_sessions = (
        owned_session_query(db, owner)
        .filter(models.Session.heartbeat <= expiry_time)
        .all()
    )

    expired_jobs, expiry_events = [], []
    for session in expired_sessions:
        zombies = session.jobs.filter(models.Job.state == "RUNNING")
        jobs, events = update_states_by_query(
            zombies,
            state="RUN_TIMEOUT",
            data="Session expired: lost contact with launcher",
        )
        expired_jobs.extend(jobs)
        expiry_events.extend(events)
    sess_ids = [sess.id for sess in expired_sessions]
    db.query(models.Session).filter(models.Session.id.in_(sess_ids)).delete(
        synchronize_session=False
    )
    db.flush()
    return expired_jobs, expiry_events


def create(db, owner, session):
    expired_jobs, expiry_events = _clear_stale_sessions(db, owner)

    site_id = (
        db.query(models.Site.id)
        .filter(models.Site.owner_id == owner.id, models.Site.id == session.site_id)
        .scalar()
    )
    if site_id is None:
        raise ValidationError(f"No site with ID {session.site_id}")
    if session.batch_job_id is not None:
        batch_job_id = (
            db.query(models.BatchJob.id)
            .filter(models.BatchJob.site_id == site_id)
            .filter(models.BatchJob.id == session.batch_job_id)
        ).scalar()
        if batch_job_id is None:
            raise ValidationError(f"No batch_job with id {session.batch_job_id}")

    created_session = models.Session(**session.dict())
    db.add(created_session)
    db.flush()
    return created_session, expired_jobs, expiry_events


def acquire(db, owner, session_id, spec: schemas.SessionAcquire):
    expired_jobs, expiry_events = _clear_stale_sessions(db, owner)
    session = (
        owned_session_query(db, owner).filter(models.Session.id == session_id)
    ).one()
    session.heartbeat = datetime.utcnow()

    qs = db.query(models.Job).join(models.App)
    qs = qs.options(orm.selectinload(models.Job.parents).load_only(models.Job.id))
    qs = qs.filter(models.App.site_id == session.site_id)  # At site
    qs = qs.filter(models.Job.session_id.is_(None))  # Unlocked
    qs = qs.filter(models.Job.state.in_(spec.states))  # Matching states
    qs = qs.filter(models.Job.tags.contains(spec.filter_tags))  # Matching tags
    qs = qs.filter(models.Job.wall_time_min <= spec.max_wall_time_min)  # By time
    qs = qs.with_for_update(of=models.Job, skip_locked=True)

    acquired_jobs = []
    for job_spec in spec.acquire:
        if job_spec.serial_only:
            jobs = qs.filter(models.Job.num_nodes == 1, models.Job.ranks_per_node == 1)
            jobs = jobs.order_by(
                models.Job.node_packing_count, models.Job.wall_time_min.desc()
            )
        else:
            jobs = qs.filter(
                models.Job.num_nodes <= job_spec.max_nodes,
                models.Job.num_nodes >= job_spec.min_nodes,
            )
            jobs = jobs.order_by(
                models.Job.num_nodes.desc(), models.Job.wall_time_min.desc()
            )
        for job in jobs[: job_spec.max_num_acquire]:
            job.session_id = session.id
            job.batch_job_id = session.batch_job_id
            acquired_jobs.append(job)

    for job in acquired_jobs:
        job.parent_ids = [parent.id for parent in job.parents]
    db.flush()
    return acquired_jobs, expired_jobs, expiry_events


def tick(db, owner, session_id):
    expired_jobs, expiry_events = _clear_stale_sessions(db, owner)
    in_db = owned_session_query(db, owner).filter(models.Session.id == session_id).one()
    ts = datetime.utcnow()
    in_db.heartbeat = ts
    db.flush()
    return ts, expired_jobs, expiry_events


def delete(db, owner, session_id):
    qs = owned_session_query(db, owner).filter(models.Session.id == session_id)
    qs.one()
    db.query(models.Session).filter(models.Session.id == session_id).delete()
    db.flush()
    return
