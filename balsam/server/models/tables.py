from datetime import datetime
from typing import Any, Dict, List, Optional

from sqlalchemy import (
    JSON,
    Boolean,
    Column,
    DateTime,
    Enum,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    Table,
    Text,
    UniqueConstraint,
    func,
    orm,
    text,
)
from sqlalchemy.dialects import postgresql as pg

from balsam.schemas.transfer import TransferDirection, TransferItemState

from .base import Base

# PK automatically has nullable=False, autoincrement
# Postgres auto-creates index for unique constraint and primary key constraint


class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True)
    username = Column(String(100), unique=True)
    hashed_password = Column(String(128), nullable=True, default=None)


class DeviceCodeAttempt(Base):
    __tablename__ = "device_code_attempts"

    client_id = Column(pg.UUID(as_uuid=True), primary_key=True)
    expiration = Column(DateTime, nullable=False, default=datetime.utcnow)
    device_code = Column(String(1024))
    user_code = Column(String(16), unique=True)
    scope = Column(String(128))
    user_denied = Column(Boolean, default=False)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=True, default=None)
    user = orm.relationship(User)


class AuthorizationState(Base):
    __tablename__ = "auth_states"
    id = Column(Integer, primary_key=True)
    state = Column(String(512), unique=True)


class Site(Base):
    __tablename__ = "sites"
    __table_args__ = (UniqueConstraint("hostname", "path"),)

    id = Column(Integer, primary_key=True)
    hostname = Column(String(100))
    path = Column(String(512))
    last_refresh = Column(DateTime)
    creation_date = Column(DateTime, default=datetime.utcnow)
    owner_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), index=True)
    globus_endpoint_id = Column(pg.UUID(as_uuid=True))

    backfill_windows = Column(JSON, default=list)
    queued_jobs = Column(JSON, default=list)
    optional_batch_job_params: Dict[str, Any] = Column(JSON, default=dict)  # type: ignore
    allowed_projects = Column(JSON, default=list)
    allowed_queues: Dict[str, Any] = Column(JSON, default=dict)  # type: ignore
    transfer_locations: Dict[str, Any] = Column(pg.JSONB, default=dict)  # type: ignore

    owner = orm.relationship(User, lazy="raise")
    apps = orm.relationship(
        "App",
        back_populates="site",
        lazy="dynamic",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )
    batch_jobs = orm.relationship(
        "BatchJob",
        back_populates="site",
        lazy="dynamic",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )
    sessions = orm.relationship(
        "Session",
        back_populates="site",
        lazy="dynamic",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )


class App(Base):
    __tablename__ = "apps"
    __table_args__ = (UniqueConstraint("site_id", "class_path"),)

    id = Column(Integer, primary_key=True)
    site_id = Column(Integer, ForeignKey("sites.id", ondelete="CASCADE"))
    description = Column(Text)
    class_path = Column(String(200), nullable=False)
    parameters: Dict[str, Any] = Column(JSON, default=dict)  # type: ignore
    transfers: Dict[str, Any] = Column(JSON, default=dict)  # type: ignore
    last_modified = Column(Float, default=0.0)

    site = orm.relationship(Site, back_populates="apps")
    jobs = orm.relationship("Job", back_populates="app", cascade="all, delete-orphan", passive_deletes=True)


# Junction table
job_deps = Table(
    "job_deps",
    Base.metadata,
    Column(
        "parent_id",
        ForeignKey("jobs.id", ondelete="CASCADE"),
        index=True,
        primary_key=True,
    ),
    Column(
        "child_id",
        ForeignKey("jobs.id", ondelete="CASCADE"),
        index=True,
        primary_key=True,
    ),
)


class Job(Base):
    __tablename__ = "jobs"

    id = Column(Integer, primary_key=True, index=True)
    workdir = Column(String(256), nullable=False)

    # https://www.postgresql.org/docs/9.4/datatype-json.html
    # We want tag-based queries to test for containment
    #     SELECT ... WHERE jobs.tags @> '{"myKey": "myValue"}'::jsonb
    # Ensure that GIN index is created:
    #     CREATE INDEX idxgin ON jobs USING gin (tags);
    tags = Column(pg.JSONB, default=dict, index=True)
    __table_args__ = (Index("ix_jobs_tags", text("(tags jsonb_path_ops)"), postgresql_using="GIN"),)

    app_id = Column(Integer, ForeignKey("apps.id", ondelete="CASCADE"))
    session_id = Column(
        Integer,
        ForeignKey("sessions.id", ondelete="SET NULL"),
        nullable=True,
        default=None,
    )
    parameters = Column(pg.JSONB, default=dict)
    batch_job_id = Column(Integer, ForeignKey("batch_jobs.id", ondelete="SET NULL"), nullable=True)
    state = Column(String(32), index=True)
    last_update = Column(DateTime(timezone=True), default=func.now(), onupdate=func.now())
    data = Column(JSON)
    return_code = Column(Integer)
    pending_file_cleanup = Column(Boolean, default=True)

    parents = orm.relationship(
        "Job",
        secondary=job_deps,
        primaryjoin=(id == job_deps.c.child_id),
        secondaryjoin=(id == job_deps.c.parent_id),
        back_populates="children",
    )
    parent_ids: Optional[List[int]]
    children = orm.relationship(
        "Job",
        secondary=job_deps,
        primaryjoin=(id == job_deps.c.parent_id),
        secondaryjoin=(id == job_deps.c.child_id),
        back_populates="parents",
        lazy="raise",
    )

    num_nodes = Column(Integer)
    ranks_per_node = Column(Integer)
    threads_per_rank = Column(Integer)
    threads_per_core = Column(Integer)
    gpus_per_rank = Column(Float)
    node_packing_count = Column(Integer)
    wall_time_min = Column(Integer)
    launch_params = Column(JSON)

    app = orm.relationship("App", back_populates="jobs")
    session: Optional["Session"] = orm.relationship("Session", back_populates="jobs")  # type: ignore
    batch_job = orm.relationship("BatchJob", back_populates="jobs")
    transfer_items: List["TransferItem"] = orm.relationship(
        "TransferItem",
        lazy="raise",
        back_populates="job",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )  # type: ignore
    log_events = orm.relationship(
        "LogEvent",
        lazy="raise",
        back_populates="job",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )


class BatchJob(Base):
    __tablename__ = "batch_jobs"

    id = Column(Integer, primary_key=True)
    site_id = Column(Integer, ForeignKey("sites.id", ondelete="CASCADE"), nullable=False)
    scheduler_id = Column(Integer, nullable=True)
    project = Column(String(64), nullable=False)
    queue = Column(String(64), nullable=False)
    optional_params = Column(JSON, default=dict)
    num_nodes = Column(Integer, nullable=False)
    wall_time_min = Column(Integer, nullable=False)
    job_mode = Column(String(16), nullable=False)
    partitions = Column(JSON, default=None, nullable=True)
    filter_tags = Column(pg.JSONB, default=dict)
    state = Column(String(32), index=True, nullable=False)
    status_info = Column(JSON, default=dict)
    start_time = Column(DateTime, nullable=True)
    end_time = Column(DateTime, nullable=True)

    site = orm.relationship(Site, back_populates="batch_jobs")
    jobs = orm.relationship("Job", back_populates="batch_job")
    sessions = orm.relationship("Session", back_populates="batch_job")


class Session(Base):
    __tablename__ = "sessions"

    id = Column(Integer, primary_key=True)
    heartbeat = Column(DateTime, default=datetime.utcnow)
    batch_job_id = Column(Integer, ForeignKey("batch_jobs.id", ondelete="SET NULL"), nullable=True)
    site_id = Column(Integer, ForeignKey("sites.id", ondelete="CASCADE"), nullable=False)

    batch_job = orm.relationship(BatchJob, lazy="raise", back_populates="sessions")
    site = orm.relationship(Site, lazy="raise", back_populates="sessions")
    jobs: "orm.Query[Job]" = orm.relationship("Job", lazy="dynamic", back_populates="session")  # type: ignore


class TransferItem(Base):
    __tablename__ = "transfer_items"

    id = Column(Integer, primary_key=True)
    job_id = Column(Integer, ForeignKey("jobs.id", ondelete="CASCADE"), nullable=False)
    direction = Column(Enum(TransferDirection), nullable=False)
    local_path = Column(String(256))
    remote_path = Column(String(256))
    recursive = Column(Boolean, default=False)
    location_alias = Column(String(256))
    state = Column(Enum(TransferItemState), nullable=False)
    task_id = Column(String(100))
    transfer_info = Column(JSON, default=dict)

    job = orm.relationship(Job, back_populates="transfer_items")


class LogEvent(Base):
    __tablename__ = "log_events"

    id = Column(Integer, primary_key=True)
    job_id = Column(Integer, ForeignKey("jobs.id", ondelete="CASCADE"))
    timestamp = Column(DateTime)
    from_state = Column(String(32))
    to_state = Column(String(32))
    data = Column(pg.JSONB, default=dict)

    job = orm.relationship(Job, back_populates="log_events")
