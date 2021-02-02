from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

from pydantic import BaseModel, Field, validator


class JobTransferItem(BaseModel):
    location_alias: str
    path: Path

    @validator("path")
    def is_absolute(cls, v: Path) -> Path:
        if not v.is_absolute():
            raise ValueError("Must provide absolute path")
        return v


class JobState(str, Enum):
    created = "CREATED"
    reset = "RESET"
    awaiting_parents = "AWAITING_PARENTS"
    ready = "READY"
    staged_in = "STAGED_IN"
    preprocessed = "PREPROCESSED"
    running = "RUNNING"
    run_done = "RUN_DONE"
    run_error = "RUN_ERROR"
    run_timeout = "RUN_TIMEOUT"
    restart_ready = "RESTART_READY"
    postprocessed = "POSTPROCESSED"
    staged_out = "STAGED_OUT"
    job_finished = "JOB_FINISHED"
    failed = "FAILED"

    @classmethod
    def is_valid(cls, state: str) -> bool:
        return state in cls._value2member_map_


RUNNABLE_STATES = {JobState.preprocessed, JobState.restart_ready}


class JobBase(BaseModel):
    workdir: Path = Field(..., example="test_jobs/test1")
    tags: Dict[str, str] = Field({}, example={"system": "H2O"})
    parameters: Dict[str, str] = Field({}, example={"input_file": "input.dat"})
    data: Dict[str, Any] = Field({}, example={"energy": -0.5})
    return_code: Optional[int] = Field(None, example=0)

    num_nodes: int = Field(1, example=1)
    ranks_per_node: int = Field(1, example=1)
    threads_per_rank: int = Field(1, example=1)
    threads_per_core: int = Field(1, example=1)
    launch_params: Dict[str, str] = Field({}, example={"cpu_affinity": "depth"})
    gpus_per_rank: float = Field(0, example=0.5)
    node_packing_count: int = Field(1, example=12)
    wall_time_min: int = Field(0, example=30)

    @validator("workdir")
    def path_is_relative(cls, v: Path) -> Path:
        if v.is_absolute():
            raise ValueError("Cannot use absolute path")
        return v


class JobCreate(JobBase):
    app_id: int = Field(..., example=3)
    parent_ids: Set[int] = Field({}, example={2, 3})
    transfers: Dict[str, JobTransferItem] = Field(
        {},
        example={
            "input_file": {
                "location_alias": "MyCluster",
                "path": "/path/to/input.dat",
            }
        },
    )


class JobUpdate(JobBase):
    workdir: Path = Field(None, example="test_jobs/test1")
    batch_job_id: int = Field(None, example=4)
    state: JobState = Field(None, example="RESET")
    state_timestamp: datetime = Field(None)
    state_data: Dict[str, Any] = Field({})


class JobBulkUpdate(JobUpdate):
    id: int = Field(..., example=123)


class JobOut(JobBase):
    id: int = Field(..., example=22)
    app_id: int = Field(..., example=3)
    parent_ids: Set[int] = Field(..., example={20, 21})
    batch_job_id: int = Field(None, example=4)
    last_update: datetime = Field(...)
    state: JobState = Field(..., example="JOB_FINISHED")

    class Config:
        orm_mode = True


class PaginatedJobsOut(BaseModel):
    count: int
    results: List[JobOut]
