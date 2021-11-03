from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Union

from pydantic import BaseModel, Field, root_validator, validator

from .serializer import serialize

# Set limits to keep queries performant *and* respect the constraints
# of maximum `argv` size that can be passed into a subprocess
# (We can get away with ~1.5MB payload delivered via 128K chunksize args)
MAX_SERIALIZED_PARAMS_SIZE = 512_000


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
DONE_STATES = {JobState.job_finished, JobState.failed}


class JobBase(BaseModel):
    workdir: Path = Field(..., example="test_jobs/test1", description="Job path relative to site data/ folder.")
    tags: Dict[str, str] = Field({}, example={"system": "H2O"}, description="Custom key:value string tags.")
    serialized_parameters: str = Field(
        "", description="Encoded parameters dict", no_constructor=True, no_descriptor=True
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

    @validator("serialized_parameters")
    def max_params_size(cls, v: str) -> str:
        if len(v) > MAX_SERIALIZED_PARAMS_SIZE:
            raise AssertionError(f"serialized_parameters cannot be larger than {MAX_SERIALIZED_PARAMS_SIZE}")
        return v


class JobCreate(JobBase):
    class Config:
        validate_assignment = True

    # This will generate a Job() constructor signature with `app_id` and `site_name`
    # kwargs. These are used to resolve the integer `app_id` sent to backend.
    app_id: Union[int, str] = Field(..., example=3, description="App name, ID, or class")
    site_name: Optional[str] = Field(
        None,
        example="my-site",
        description="Site name, to disambiguate app defined at multiple Sites.",
        no_export=True,
        no_descriptor=True,
    )

    # Add `parameters` to the constructor, but manage them with a custom getter/setter on
    # JobBase:
    parameters: Dict[str, Any] = Field(
        {},
        example={"name": "world"},
        description="Parameters passed to App at runtime.",
        no_descriptor=True,
        no_export=True,
    )
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

    @root_validator(pre=True)
    def serialize_parameters(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        params = values.get("parameters", {})
        values["serialized_parameters"] = serialize(params)
        return values

    @validator("transfers", pre=True)
    def resolve_transfer_strings(
        cls, transfers: Dict[str, Union[str, JobTransferItem]]
    ) -> Dict[str, JobTransferItem]:
        if not isinstance(transfers, dict):
            raise AssertionError("transfers must be of type dict")
        result = {}
        for k, v in transfers.items():
            if isinstance(v, str):
                loc, path = v.split(":", 1)
                result[k] = JobTransferItem(location_alias=loc, path=path)
            else:
                result[k] = v
        return result

    def dict(self, **kwargs: Any) -> Dict[str, Any]:
        # Overriden to avoid sending "no_export" fields over the wire
        exclude_fields = {f for f, v in JobCreate.__fields__.items() if v.field_info.extra.get("no_export")}
        if kwargs.get("exclude") is None:
            kwargs["exclude"] = exclude_fields
        elif isinstance(kwargs["exclude"], set):
            kwargs["exclude"].update(exclude_fields)
        else:
            for key in exclude_fields:
                kwargs["exclude"][key] = ...
        return super().dict(**kwargs)


class ServerJobCreate(JobBase):
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


class JobUpdate(JobBase):
    workdir: Path = Field(None, example="test_jobs/test1", description="Job path relative to the site data/ folder")
    batch_job_id: int = Field(None, example=4, description="ID of most recent BatchJob in which this Job ran")
    state: JobState = Field(None, example="JOB_FINISHED", description="Job state")
    state_timestamp: datetime = Field(None, description="Time (UTC) at which Job state change occured")
    state_data: Dict[str, Any] = Field({}, description="Arbitrary associated state change data for logging")
    pending_file_cleanup: bool = Field(None, description="Whether job remains to have workdir cleaned.")
    serialized_parameters: str = Field(None, description="Encoded parameters dict", no_descriptor=True)
    serialized_return_value: str = Field(None, description="Encoded return value", no_descriptor=True)
    serialized_exception: str = Field(None, description="Encoded wrapped Exception", no_descriptor=True)

    @validator("serialized_return_value")
    def max_retval_size(cls, v: str) -> str:
        if len(v) > MAX_SERIALIZED_PARAMS_SIZE:
            raise AssertionError(f"serialized_return_value cannot be larger than {MAX_SERIALIZED_PARAMS_SIZE}")
        return v

    @validator("serialized_exception")
    def max_except_size(cls, v: str) -> str:
        if len(v) > MAX_SERIALIZED_PARAMS_SIZE:
            raise AssertionError(f"serialized_except cannot be larger than {MAX_SERIALIZED_PARAMS_SIZE}")
        return v


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
    serialized_parameters: str = Field(..., description="Encoded parameters dict", no_descriptor=True)
    serialized_return_value: str = Field(..., description="Encoded return value", no_descriptor=True)
    serialized_exception: str = Field(..., description="Encoded wrapped Exception", no_descriptor=True)

    class Config:
        orm_mode = True


class PaginatedJobsOut(BaseModel):
    count: int
    results: List[JobOut]
