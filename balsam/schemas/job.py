from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Union

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


class JobOrdering(str, Enum):
    last_update = "last_update"
    last_update_desc = "-last_update"
    id = "id"
    id_desc = "-id"
    state = "state"
    state_desc = "-state"
    workdir = "workdir"
    workdir_desc = "-workdir"


RUNNABLE_STATES = {JobState.preprocessed, JobState.restart_ready}


class JobBase(BaseModel):
    workdir: Path = Field(..., example="test_jobs/test1", description="Job path relative to site data/ folder.")
    tags: Dict[str, str] = Field({}, example={"system": "H2O"}, description="Custom key:value string tags.")
    parameters: Dict[str, str] = Field(
        {}, example={"input_file": "input.dat"}, description="App parameter name:value pairs."
    )
    data: Dict[str, Any] = Field({}, example={"energy": -0.5}, description="Arbitrary JSON-able data dictionary.")
    return_code: Optional[int] = Field(None, example=0, description="Return code from last execution of this Job.")

    num_nodes: int = Field(1, example=1, description="Number of compute nodes needed.")
    ranks_per_node: int = Field(1, example=1, description="Number of MPI processes per node.")
    threads_per_rank: int = Field(1, example=1, description="Logical threads per process.")
    threads_per_core: int = Field(1, example=1, description="Logical threads per CPU core.")
    launch_params: Dict[str, str] = Field(
        {},
        example={"cpu_affinity": "depth"},
        description="Optional pass-through parameters to MPI application launcher.",
    )
    gpus_per_rank: float = Field(0, example=0.5, description="Number of GPUs per process.")
    node_packing_count: int = Field(1, example=12, description="Maximum number of concurrent runs per node.")
    wall_time_min: int = Field(
        0,
        example=30,
        description="Optional estimate of Job runtime. All else being equal, longer Jobs tend to run first.",
    )

    @validator("workdir")
    def path_is_relative(cls, v: Path) -> Path:
        if v.is_absolute():
            raise ValueError("Cannot use absolute path")
        return v


class JobCreate(JobBase):
    app_id: int = Field(..., example=3, description="App ID")
    parent_ids: Set[int] = Field(set(), example={2, 3}, description="Set of parent Job IDs (dependencies).")
    transfers: Dict[str, JobTransferItem] = Field(
        {},
        example={
            "input_file": {
                "location_alias": "MyCluster",
                "path": "/path/to/input.dat",
            }
        },
        description="TransferItem dictionary. One key:JobTransferItem pair for each slot defined on the App.",
    )


class ClientJobCreate(JobBase):
    app_id: Optional[int] = Field(None, example=123, description="App ID")
    app_name: Optional[str] = Field(None, example="demo.Hello", description="App Class Name")
    site_path: Optional[str] = Field(None, example="my-polaris-site", description="Site Path Substring")
    parent_ids: Set[int] = Field(set(), example={2, 3}, description="Set of parent Job IDs (dependencies).")
    transfers: Dict[str, Union[str, JobTransferItem]] = Field(
        {},
        example={
            "input_file": {
                "location_alias": "MyCluster",
                "path": "/path/to/input.dat",
            },
            "input_file2": "APS_DTN:/path/to/inp2.dat",
        },
        description="TransferItem dictionary. One key:JobTransferItem pair for each slot defined on the App.",
    )

    @validator("transfers")
    def validate_transfers(cls, transfers: Dict[str, Union[str, JobTransferItem]]) -> Dict[str, JobTransferItem]:
        validated = {}
        for key, val in transfers.items():
            if isinstance(val, str):
                location_alias, path = val.split(":", 1)
                validated[key] = JobTransferItem(location_alias=location_alias, path=path)
            else:
                validated[key] = val
        return validated


class JobUpdate(JobBase):
    workdir: Path = Field(None, example="test_jobs/test1", description="Job path relative to the site data/ folder")
    batch_job_id: int = Field(None, example=4, description="ID of most recent BatchJob in which this Job ran")
    state: JobState = Field(None, example="JOB_FINISHED", description="Job state")
    state_timestamp: datetime = Field(None, description="Time (UTC) at which Job state change occured")
    state_data: Dict[str, Any] = Field({}, description="Arbitrary associated state change data for logging")
    pending_file_cleanup: bool = Field(None, description="Whether job remains to have workdir cleaned.")


class JobBulkUpdate(JobUpdate):
    id: int = Field(..., example=123, description="Job id")


class JobOut(JobBase):
    id: int = Field(..., example=22)
    app_id: int = Field(..., example=3)
    parent_ids: Set[int] = Field(..., example={20, 21})
    batch_job_id: int = Field(None, example=4)
    last_update: datetime = Field(...)
    state: JobState = Field(..., example="JOB_FINISHED")
    pending_file_cleanup: bool = Field(..., description="Whether job remains to have workdir cleaned.")

    class Config:
        orm_mode = True


class PaginatedJobsOut(BaseModel):
    count: int
    results: List[JobOut]
