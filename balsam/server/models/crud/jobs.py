from datetime import datetime
from balsam.server import models, ValidationError
from balsam.schemas.job import JobState
from sqlalchemy import orm, func, case, literal_column
from fastapi import HTTPException, status
from fastapi.encoders import jsonable_encoder


def owned_job_query(db, owner, with_parents=False):
    if with_parents:
        qs = db.query(models.Job).options(
            orm.selectinload(models.Job.parents).load_only(models.Job.id)
        )
    else:
        qs = db.query(models.Job)
    qs = qs.join(models.App).join(models.Site).filter(models.Site.owner_id == owner.id)
    return qs


def set_parent_ids(jobs):
    for job in jobs:
        job.parent_ids = [p.id for p in job.parents]


def update_states_by_query(qs, state, data=None, timestamp=None):
    now = datetime.utcnow()
    if isinstance(data, str):
        data = {"message": data}
    elif data is None:
        data = {}

    updated_jobs = qs.all()
    events = []
    for job in updated_jobs:
        event = _update_state(job, state, now, data)
        if event:
            events.append(event)
    return updated_jobs, events


def validate_parameters(job):
    allowed_params = set(job.app.parameters.keys())
    required_params = {p for p in allowed_params if job.app.parameters[p]["required"]}
    job_params = set(job.parameters.keys())
    extraneous_params = job_params.difference(allowed_params)
    missing_params = required_params.difference(job_params)
    if extraneous_params:
        raise ValidationError(f"extraneous parameters: {extraneous_params}")
    if missing_params:
        raise ValidationError(f"missing parameters: {missing_params}")


def populate_transfers(db_job, job_spec):
    for transfer_name, transfer_spec in job_spec.transfers.items():
        transfer_slot = db_job.app.transfers.get(transfer_name)
        if transfer_slot is None:
            raise ValidationError(
                f"App {db_job.app.name} has no Transfer slot named {transfer_name}"
            )
        direction, local_path = transfer_slot["direction"], transfer_slot["local_path"]
        recursive = transfer_slot["recursive"]
        assert direction in ["in", "out"]
        location_alias, remote_path = (
            transfer_spec.location_alias,
            transfer_spec.path.as_posix(),
        )
        url = db_job.app.site.transfer_locations.get(location_alias)
        if url is None:
            raise ValidationError(f"Site has no Transfer URL named {location_alias}")

        transfer_item = models.TransferItem(
            direction=direction,
            local_path=local_path,
            remote_path=remote_path,
            recursive=recursive,
            location_alias=location_alias,
            state="pending" if direction == "in" else "awaiting_job",
            task_id="",
            transfer_info={},
        )
        db_job.transfer_items.append(transfer_item)
    return db_job.transfer_items


def fetch(db, owner, paginator=None, job_id=None, filterset=None):
    qs = owned_job_query(db, owner, with_parents=True)
    if job_id is not None:
        qs = qs.filter(models.Job.id == job_id)
    if filterset:
        qs = filterset.apply_filters(qs)
    if paginator is None:
        job = qs.one()
        set_parent_ids([job])
        return 1, [job]
    count = qs.group_by(models.Job.id).count()
    jobs = paginator.paginate(qs)
    set_parent_ids(jobs)
    return count, jobs


def bulk_create(db, owner, job_specs):
    now = datetime.utcnow()
    app_ids = set(job.app_id for job in job_specs)
    parent_ids = set(pid for job in job_specs for pid in job.parent_ids)

    apps = {
        app.id: app
        for app in db.query(models.App)
        .join(models.App.site)
        .filter(models.Site.owner_id == owner.id, models.App.id.in_(app_ids))
        .options(
            orm.selectinload(models.App.site).load_only(models.Site.transfer_locations)
        )
        .all()
    }
    if len(apps) < len(app_ids):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Could not find one or more Apps. Double check app_id fields.",
        )

    parents = {
        job.id: job
        for job in owned_job_query(db, owner)
        .filter(models.Job.id.in_(parent_ids))
        .all()
    }
    if len(parents) < len(parent_ids):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Could not find one or more Jobs set as parents. Double check parent_ids fields.",
        )

    created_jobs, created_transfers, created_events = [], [], []
    for job_spec in job_specs:
        db_job = models.Job(
            **jsonable_encoder(job_spec.dict(exclude={"parent_ids", "transfers"}))
        )
        db.add(db_job)
        db_job.app = apps[job_spec.app_id]
        validate_parameters(db_job)

        db_job.parents.extend(parents[id] for id in job_spec.parent_ids)
        populate_transfers(db_job, job_spec)
        if any(job.state != JobState.job_finished for job in db_job.parents):
            db_job.state = JobState.awaiting_parents
        else:
            if any(tr.direction == "in" for tr in db_job.transfer_items) > 0:
                db_job.state = JobState.ready
            else:
                db_job.state = JobState.staged_in

        event = models.LogEvent(
            job=db_job,
            timestamp=now,
            from_state=JobState.created,
            to_state=db_job.state,
        )
        created_jobs.append(db_job)
        created_events.append(event)
        created_transfers.extend(db_job.transfer_items)

    set_parent_ids(created_jobs)
    db.flush()
    return created_jobs, created_events, created_transfers


def _update_state(job, state, state_timestamp, state_data):
    if state == job.state or state is None:
        return
    if state == "RESET" and job.state in ["AWAITING_PARENTS", "READY"]:
        return
    event = models.LogEvent(
        job=job,
        from_state=job.state,
        to_state=state,
        timestamp=state_timestamp,
        data=state_data,
    )
    job.state = state

    if job.state != "RUNNING" and job.session_id is not None:
        job.session = None
        job.session_id = None

    if job.state == "RESET":
        if all(parent.state == "JOB_FINISHED" for parent in job.parents):
            job.state = "READY"
        else:
            job.state = "AWAITING_PARENTS"

    if job.state == "READY":
        if all(
            item.state == "done"
            for item in job.transfer_items
            if item.direction == "in"
        ):
            job.state = "STAGED_IN"
            event.data["message"] = "Skipped stage in"

    if job.state == "POSTPROCESSED":
        if all(
            item.state == "done"
            for item in job.transfer_items
            if item.direction == "out"
        ):
            job.state = "JOB_FINISHED"
            event.data["message"] = "Skipped stage out"
        else:
            for item in job.transfer_items:
                if item.state == "awaiting_job":
                    item.state = "pending"

    if job.state == "STAGED_OUT":
        job.state = "JOB_FINISHED"

    event.to_state = job.state
    return event


def _update(job, data):
    state = data.pop("state", None)
    state_timestamp = data.pop("state_timestamp", datetime.utcnow())
    state_data = data.pop("state_data", {})

    for k, v in data.items():
        setattr(job, k, v)
    job.return_code = data.get("return_code", job.return_code)
    job.data = data.get("data", job.data)
    event = _update_state(job, state, state_timestamp, state_data)
    return event


def _check_waiting_children(db, parent_ids):
    parent_alias1 = orm.aliased(models.Job)
    parent_alias2 = orm.aliased(models.Job)
    num_parents = func.count(parent_alias1.id)
    num_finished_parents = func.count(
        case(
            [((parent_alias1.state == "JOB_FINISHED"), parent_alias1.id)],
            else_=literal_column("NULL"),
        )
    )
    ready_children = (
        db.query(models.Job)
        .options(
            orm.selectinload(models.Job.transfer_items).load_only(
                models.TransferItem.state, models.TransferItem.direction,
            ),
            # TODO: what is most efficient way to load parent IDs in same query?
            orm.joinedload(models.Job.parents).load_only(
                models.Job.id, models.Job.state,
            ),
        )
        .join(models.Job.parents.of_type(parent_alias1))
        .join(models.Job.parents.of_type(parent_alias2))
        .filter(models.Job.state == "AWAITING_PARENTS")
        .filter(parent_alias2.id.in_(parent_ids))
        .group_by(models.Job.id)
        .having(num_parents == num_finished_parents)
        .all()
    )
    now = datetime.utcnow()
    new_events = []
    for child in ready_children:
        event = _update_state(child, "READY", now, {"message": "All parents finished"})
        if event:
            new_events.append(event)
    return ready_children, new_events


def _select_jobs_for_update(qs):
    qs = qs.options(
        orm.selectinload(models.Job.parents).load_only(
            models.Job.id, models.Job.state,
        ),
        orm.selectinload(models.Job.transfer_items).load_only(
            models.TransferItem.state, models.TransferItem.direction,
        ),
    ).with_for_update(of=models.Job)
    return qs.all()


def _update_jobs(db, update_jobs, patch_dicts):
    new_events = []
    # First, perform job-wise updates:
    for job in update_jobs:
        event = _update(job, patch_dicts[job.id])
        if event:
            new_events.append(event)
    update_ids = [job.id for job in update_jobs]
    db.flush()

    # Then update affected children in a second query:
    updated_children, child_events = _check_waiting_children(db, update_ids)
    db.flush()
    update_jobs.extend(updated_children)
    new_events.extend(child_events)
    set_parent_ids(update_jobs)
    return update_jobs, new_events


def bulk_update(db, owner, patch_dicts):
    job_ids = set(patch_dicts.keys())
    qs = owned_job_query(db, owner).filter(models.Job.id.in_(job_ids))
    update_jobs = _select_jobs_for_update(qs)
    if len(update_jobs) < len(patch_dicts):
        raise ValidationError("Could not find some Job IDs")
    return _update_jobs(db, update_jobs, patch_dicts)


def update_query(db, owner, update_data, filterset):
    qs = owned_job_query(db, owner)
    qs = filterset.apply_filters(qs)
    update_jobs = _select_jobs_for_update(qs)
    patch_dicts = {job.id: update_data.copy() for job in update_jobs}
    return _update_jobs(db, update_jobs, patch_dicts)


def _select_children(db, ids):
    qs = db.query(models.Job).filter(models.Job.id.in_(ids))
    qs = qs.options(
        orm.load_only(models.Job.id),
        orm.selectinload(models.Job.children).load_only(models.Job.id),
    )
    jobs = qs.all()
    selected_ids = [job.id for job in jobs]
    child_ids = [child.id for job in jobs for child in job.children]
    if child_ids:
        selected_ids.extend(_select_children(db, child_ids))
    return selected_ids


def delete_query(db, owner, filterset=None, job_id=None):
    qs = owned_job_query(db, owner)
    if job_id is not None:
        qs = qs.filter(models.Job.id == job_id)
        qs.one()
    else:
        qs = filterset.apply_filters(qs)
    qs = qs.filter(models.Job.session_id.is_(None)).with_for_update(of=models.Job)
    ids = [job.id for job in qs.options(orm.load_only(models.Job.id))]
    delete_ids = set(_select_children(db, ids))
    print("Deleting job ids:", delete_ids)
    db.query(models.Job).filter(models.Job.id.in_(delete_ids)).delete(
        synchronize_session=False
    )
    db.flush()
    return delete_ids